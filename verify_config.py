import os
import unittest
from pspf.settings import Settings
from pspf.config import load_config_from_file

class TestConfig(unittest.TestCase):
    def test_default_settings(self):
        s = Settings()
        self.assertEqual(s.valkey.HOST, "localhost")
        self.assertEqual(s.telemetry.PROMETHEUS_PORT, 8000)
    
    def test_env_override(self):
        os.environ["VALKEY_HOST"] = "remote-redis"
        os.environ["OTEL_ENABLED"] = "True"
        s = Settings()
        self.assertEqual(s.valkey.HOST, "remote-redis")
        self.assertTrue(s.telemetry.ENABLED)

    def test_yaml_load(self):
        import yaml
        config_data = {
            "valkey": {
                "HOST": "yaml-host",
                "PORT": 7777
            },
            "telemetry": {
                "ADMIN_PORT": 9999
            }
        }
        with open("test_config.yaml", "w") as f:
            yaml.dump(config_data, f)
        
        s = load_config_from_file("test_config.yaml")
        self.assertEqual(s.valkey.HOST, "yaml-host")
        self.assertEqual(s.valkey.PORT, 7777)
        self.assertEqual(s.telemetry.ADMIN_PORT, 9999)
        
        os.remove("test_config.yaml")

if __name__ == "__main__":
    unittest.main()
