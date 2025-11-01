"""
ConfigManager for OmniCart Pipeline
Reads settings from pipeline.cfg.txt (INI format)
"""
import configparser
import os

class ConfigManager:
    def __init__(self, config_path: str = None):
        if config_path is None:
            # Default location in project root
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pipeline.cfg.txt')
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self._load()

    def _load(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        self.config.read(self.config_path)

    @property
    def base_url(self) -> str:
        return self.config.get('API', 'base_url', fallback='https://fakestoreapi.com')

    @property
    def limit(self) -> int:
        return self.config.getint('API', 'limit', fallback=5)

if __name__ == "__main__":
    cfg = ConfigManager()
    print("Base URL:", cfg.base_url)
    print("Limit:", cfg.limit)
