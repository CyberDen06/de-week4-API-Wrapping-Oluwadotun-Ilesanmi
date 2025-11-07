"""ConfigManager for the API Wrapping pipeline.

Reads settings from a pipeline.cfg INI file and exposes them as properties.
"""

import configparser
import os
from typing import Optional


class ConfigManager:
	def __init__(self, config_path: Optional[str] = None):
		# Default to pipeline.cfg in the same directory as this file
		if config_path is None:
			config_path = os.path.join(os.path.dirname(__file__), 'pipeline.cfg')
		self.config_path = config_path
		self._parser = configparser.ConfigParser()
		self._load()

	def _load(self) -> None:
		if not os.path.exists(self.config_path):
			raise FileNotFoundError(f"Config file not found: {self.config_path}")
		self._parser.read(self.config_path)

	def get(self, section: str, option: str, fallback: Optional[str] = None) -> Optional[str]:
		return self._parser.get(section, option, fallback=fallback)

	@property
	def base_url(self) -> str:
		return self._parser.get('API', 'base_url', fallback='https://fakestoreapi.com')

	@property
	def limit(self) -> int:
		return self._parser.getint('API', 'limit', fallback=5)


if __name__ == '__main__':
	cfg = ConfigManager()
	print('Config file:', cfg.config_path)
	print('Base URL:', cfg.base_url)
	print('Limit:', cfg.limit)

