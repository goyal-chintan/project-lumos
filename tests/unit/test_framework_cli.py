"""Unit tests for framework CLI."""
import pytest
from unittest.mock import patch, Mock
import sys


class TestFrameworkCLI:
    """Test cases for framework CLI."""
    
    @patch('framework_cli.IngestionService')
    @patch('framework_cli.ConfigManager')
    @patch('framework_cli.PlatformFactory')
    def test_run_ingestion_success(self, mock_factory, mock_config, mock_ingestion):
        """Test successful ingestion run."""
        # Setup mocks
        mock_config_instance = Mock()
        mock_config_instance.load_config.return_value = {
            "sink": {"type": "datahub"}
        }
        mock_config_instance.get_global_config.return_value = {
            "datahub": {"gms_server": "http://localhost:8080"}
        }
        mock_config.return_value = mock_config_instance
        
        mock_platform = Mock()
        mock_factory.get_instance.return_value = mock_platform
        
        mock_service_instance = Mock()
        mock_service_instance.start_ingestion.return_value = True
        mock_ingestion.return_value = mock_service_instance
        
        # Import after mocking
        from framework_cli import run_ingestion
        
        # Run the function
        run_ingestion("test_config.yaml")
        
        # Assertions
        mock_config_instance.load_config.assert_called_once_with("test_config.yaml")
        mock_service_instance.start_ingestion.assert_called_once_with("test_config.yaml")
    
    def test_cli_requires_command(self):
        """Test that CLI requires a command."""
        with patch('sys.argv', ['framework_cli.py']):
            with pytest.raises(SystemExit):
                from framework_cli import main
                main()

