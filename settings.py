class Settings:
    instance = None

    def __init__(self):
        # url for CVAT server
        self.cvat_api_base_url = 'https://cvat.ai'

        # url for CVAT server to send webhook notification
        self.webhook_target_url = 'http://localhost:1235'

        # notification web socket server will be launched on this address
        # for transferring webhook notifications from CVAT to its subscribers
        self.notification_server_url = 'ws://localhost:1234'

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            cls.instance = Settings()

        return cls.instance
