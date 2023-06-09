"""Module for testing log_analyzer"""
from datetime import datetime
from unittest import TestCase, main

import log_analyzer


class AnalyzerTest(TestCase):
    """Analyzer test"""
    def test_base_config(self):
        """config testing"""
        self.assertIsNotNone(log_analyzer.config)
        self.assertIsInstance(log_analyzer.config, dict)

        self.assertIsNotNone(log_analyzer.config.get('LOG_DIR'))
        self.assertIsNotNone(log_analyzer.config.get('REPORT_DIR'))
        self.assertIsNotNone(log_analyzer.config.get('REPORT_SIZE'))

        self.assertIsInstance(log_analyzer.config.get('REPORT_SIZE'), int)
        self.assertGreater(log_analyzer.config.get('REPORT_SIZE'), 0)

    def test_get_last_logfile(self):
        """logfile testing"""
        log_file = log_analyzer.get_last_logfile(log_analyzer.config.get('LOG_DIR'))
        print(log_file)
        self.assertIsInstance(
            log_file.date,
            datetime
        )
        self.assertIsInstance(
            log_file.name,
            str
        )
        self.assertGreater(
            len(log_file.name),
            0
        )
        self.assertIn(
            'ui',
            log_file.name,
        )


if __name__ == '__main__':
    main()
