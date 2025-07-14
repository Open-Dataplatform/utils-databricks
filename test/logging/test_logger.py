import pytest
from custom_utils import Logger

class TestLogger:
    def setup_method(self):
        self.logger: Logger = Logger()
    
    def teardown_method(self):
        del self.logger

    @pytest.mark.parametrize("debug, log_level", [(True, 10), (False, 20)])
    def test_set_level(self, debug: bool, log_level: int):
        self.logger.set_level(debug)
        assert self.logger.logger.level == log_level

    @pytest.mark.parametrize("debug, log_level", [(True, 10), (False, 20)])   
    def test_update_debug_mode(self, debug: bool, log_level: int):
        self.logger.update_debug_mode(debug=debug)
        assert self.logger.logger.level == log_level
