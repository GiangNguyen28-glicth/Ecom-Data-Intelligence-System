import pyspark.sql.functions as F
import pyspark.sql.types as T
from adapters.iceberg_spark_adapter import iceberg_spark_adapter
from helpers.helpers import Helper

# =====================================================================
# BẢNG ÁNH XẠ dataset -> đơn vị mặc định
# =====================================================================
DATASET_DEFAULT_UNIT = {
    "us_dea_fugitives": "lb",
    "us_fbi_most_wanted": "lb",
    "us_ss_wanted": "lb",
    "us_ice_wanted": "lb",
    "interpol_red_notices": "kg",
    "za_wanted": "kg",
}

LB_TO_KG = 0.453592
MIN_VALID_KG = 20
MAX_VALID_KG = 300


class OpensanctionsValidateWeight:
    def __init__(self):
        self.spark = iceberg_spark_adapter.spark

    def _sanity_check(self, col_expr):
        """Hàm helper để filter và round số liệu trọng lượng natively."""
        return F.when(
            col_expr.isNotNull() & (col_expr >= MIN_VALID_KG) & (col_expr <= MAX_VALID_KG),
            F.round(col_expr, 1)
        ).otherwise(F.lit(None).cast("double"))

    def process(self, config):
        bucket = Helper.get_bucket(config["bucket"])

        raw_df = self.spark.read.json(f"{bucket}/entities.ftm.json")

        parsed_df = (
            raw_df
            .select(
                F.col("id").alias("profileCode"),
                F.col("schema").alias("profileType"),
                F.col("datasets").alias("datasets"),
                F.col("properties.weight").alias("weightRawArray"),
            )
            .filter(F.col("weightRawArray").isNotNull())
        )

        # 1. EXPLODE NGAY TỪ ĐẦU (Biến Array -> Dòng đơn để dễ xử lý)
        exploded_df = parsed_df.select(
            "profileCode", "profileType", "datasets",
            F.explode("weightRawArray").alias("weightRaw")
        )

        # 2. LÀM SẠCH CHUỖI (CLEANING)
        # Bỏ "approx.", khoảng trắng và dấu chấm ở đuôi
        df_clean = exploded_df.withColumn(
            "clean",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(F.col("weightRaw"), r"(?i)^\s*approx(imately)?\.?\s*", ""),
                    r"\.\s*$", ""
                )
            )
        )

        # 3. TRÍCH XUẤT REGEX SONG SONG (Sử dụng Non-capturing group và Index groups của Java Regex)
        df_regex = df_clean.select(
            "*",
            F.regexp_extract("clean", r"(?i)\(([\d.,]+)\s*kgs?\)", 1).alias("r_dual"),
            F.regexp_extract("clean", r"(?i)^([\d.,]+)\s*to\s*([\d.,]+)\s*(kgs?|lbs?|pounds?)?$", 1).alias("r_rng1"),
            F.regexp_extract("clean", r"(?i)^([\d.,]+)\s*to\s*([\d.,]+)\s*(kgs?|lbs?|pounds?)?$", 2).alias("r_rng2"),
            F.regexp_extract("clean", r"(?i)^([\d.,]+)\s*to\s*([\d.,]+)\s*(kgs?|lbs?|pounds?)?$", 3).alias("r_rngu"),
            F.regexp_extract("clean", r"(?i)^([\d.,]+)\s*kgs?(?:\s*kgs?)?\.?$", 1).alias("r_kg"),
            F.regexp_extract("clean", r"(?i)^([\d.,]+)\s*(?:lbs?|pounds?)\.?$", 1).alias("r_lb"),
            F.regexp_extract("clean", r"^([\d.,]+)$", 1).alias("r_raw"),
        )

        # 4. CHUYỂN ĐỔI CHUỖI THÀNH SỐ
        def str_to_double(col_name):
            return F.regexp_replace(F.col(col_name), ",", "").cast("double")

        c_dual = str_to_double("r_dual")
        c_rng1 = str_to_double("r_rng1")
        c_rng2 = str_to_double("r_rng2")
        c_kg = str_to_double("r_kg")
        c_lb = str_to_double("r_lb")
        c_raw = str_to_double("r_raw")

        # 5. ĐỊNH NGHĨA CÁC ĐIỀU KIỆN RẼ NHÁNH
        c_empty = (F.col("clean") == "") | (F.upper(F.col("clean")) == "N/A")
        is_dual = (F.col("r_dual") != "")
        is_rng = (F.col("r_rng1") != "")
        is_kg = (F.col("r_kg") != "")
        is_lb = (F.col("r_lb") != "")
        is_raw = (F.col("r_raw") != "")

        is_rng_lb = F.lower(F.col("r_rngu")).rlike("lbs?|pounds?")
        is_rng_kg = F.lower(F.col("r_rngu")).rlike("kgs?")

        # 6. LOOKUP DATASET UNIT TRỰC TIẾP TRONG SQL
        # Tạo Map SQL từ Bảng Python: map('us_dea_fugitives', 'lb', ...)
        map_sql = "map(" + ", ".join([f"'{k}', '{v}'" for k, v in DATASET_DEFAULT_UNIT.items()]) + ")"

        # Tìm dataset đầu tiên trong mảng 'datasets' trùng với key của Map
        dataset_map_expr = F.expr(f"element_at(filter(datasets, d -> {map_sql}[d] is not null), 1)")
        df_regex = df_regex.withColumn("ds_matched", dataset_map_expr)
        df_regex = df_regex.withColumn("ds_unit", F.expr(f"{map_sql}[ds_matched]"))

        # 7. TÍNH TOÁN CỘT KẾT QUẢ BẰNG F.WHEN()
        final_df = df_regex.withColumn(
            "weightKg",
            F.when(c_empty, F.lit(None))
            .when(is_dual, self._sanity_check(c_dual))
            .when(is_rng & is_rng_lb, self._sanity_check((c_rng1 * LB_TO_KG + c_rng2 * LB_TO_KG) / 2))
            .when(is_rng & is_rng_kg, self._sanity_check((c_rng1 + c_rng2) / 2))
            .when(is_rng, F.lit(None))  # range_no_unit => Không có giá trị cố định
            .when(is_kg, self._sanity_check(c_kg))
            .when(is_lb, self._sanity_check(c_lb * LB_TO_KG))
            .when(is_raw & (F.col("ds_unit") == "kg"), self._sanity_check(c_raw))
            .when(is_raw & (F.col("ds_unit") == "lb"), self._sanity_check(c_raw * LB_TO_KG))
            .otherwise(F.lit(None).cast("double"))
        ).withColumn(
            "weightKgMin",
            F.when(is_rng & is_rng_lb, self._sanity_check(c_rng1 * LB_TO_KG))
            .when(is_rng & is_rng_kg, self._sanity_check(c_rng1))
            .otherwise(F.lit(None).cast("double"))
        ).withColumn(
            "weightKgMax",
            F.when(is_rng & is_rng_lb, self._sanity_check(c_rng2 * LB_TO_KG))
            .when(is_rng & is_rng_kg, self._sanity_check(c_rng2))
            .otherwise(F.lit(None).cast("double"))
        ).withColumn(
            "weightUnitConfidence",
            F.when(c_empty, "unparseable")
            .when(is_dual, "explicit_kg")
            .when(is_rng & is_rng_lb, "explicit_lb")
            .when(is_rng & is_rng_kg, "explicit_kg")
            .when(is_rng, "range_no_unit")
            .when(is_kg, "explicit_kg")
            .when(is_lb, "explicit_lb")
            .when(is_raw & (F.col("ds_unit") == "kg"), "dataset_inferred_kg")
            .when(is_raw & (F.col("ds_unit") == "lb"), "dataset_inferred_lb")
            .when(is_raw, "unknown_dataset")
            .otherwise("unparseable")
        )

        # 8. LỌC LẠI CHỈ GIỮ NHỮNG CỘT CẦN THIẾT
        result_df = final_df.select(
            "profileCode", "profileType", "datasets", "weightRaw",
            "weightKg", "weightKgMin", "weightKgMax", "weightUnitConfidence"
        )

        # ==========================================================
        # PHẦN LỌC SUCCESS VÀ REVIEW Y HỆT NHƯ CŨ
        # ==========================================================
        UNRESOLVED_CONFIDENCE = ("unparseable", "unknown_dataset", "range_no_unit")

        success_df = result_df.filter(
            F.col("weightKg").isNotNull()
            & ~F.col("weightUnitConfidence").isin(*UNRESOLVED_CONFIDENCE)
        )

        review_df = result_df.filter(
            F.col("weightKg").isNull()
            | F.col("weightUnitConfidence").isin(*UNRESOLVED_CONFIDENCE)
        )

        total = result_df.count()
        n_success = success_df.count()
        n_review = review_df.count()

        print("=== THONG KE TONG QUAN ===")
        print(f"Tong so gia tri weight: {total}")
        if total > 0:
            print(f"THANH CONG (co weightKg ro rang): {n_success} ({n_success / total:.1%})")
            print(f"CAN REVIEW: {n_review} ({n_review / total:.1%})")

        print("\n=== PHAN BO weightUnitConfidence (TAP THANH CONG) ===")
        success_df.groupBy("weightUnitConfidence").count().orderBy(F.desc("count")).show(truncate=False)

        print("\n=== DATASET NAO DANG GAY REVIEW NHIEU NHAT ===")
        (
            review_df
            .withColumn("dataset", F.explode(F.col("datasets")))
            .groupBy("dataset", "weightUnitConfidence")
            .count()
            .orderBy(F.desc("count"))
            .show(50, truncate=False)
        )
        output_success_path = "s3a://output-validate/opensanctions/weight/success"
        output_review_path = "s3a://output-validate/opensanctions/weight/review"
        success_df.write.mode("overwrite").parquet(output_success_path)
        review_df.write.mode("overwrite").parquet(output_review_path)
        return {
            "total": total,
            "success": n_success,
            "review": n_review,
        }