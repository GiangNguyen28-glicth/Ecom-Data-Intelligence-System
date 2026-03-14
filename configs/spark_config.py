class SparkConfig:
    def __init__(self, app_name = "Spark App", master = 'local[*]'):
        self.app_name = app_name
        self.master = master