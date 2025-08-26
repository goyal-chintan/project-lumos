# framework_cli.py
import argparse
import logging
from core.controllers import ingestion_controller, lineage_controller, data_job_lineage_controller, enrichment_controller, version_controller, ownership_controller
from feature.extraction.extraction_factory import ExtractionFactory
from feature.extraction.export.excel_exporter import ExcelExporter
from feature.extraction.export.csv_exporter import CSVExporter
from feature.extraction.export.visualization_exporter import VisualizationExporter
from core.common.config_manager import ConfigManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(
        description="🚀 DataHub Framework CLI v2.0.0 - Comprehensive Data Management Toolkit",
        epilog="""
📋 OPERATION CATEGORIES:

📥 INGESTION OPERATIONS:
  ingest:config.json                    Ingest metadata from data sources (CSV, Avro, Parquet, MongoDB, S3, PostgreSQL)

🔗 LINEAGE OPERATIONS:
  add-lineage:config.json              Add dataset lineage relationships
  add-data-job-lineage:config.json     Add data job lineage relationships

🏷️ ENRICHMENT OPERATIONS:
  enrich:config.json                   Enrich datasets with tags, descriptions, properties, documentation

👤 OWNERSHIP OPERATIONS:
  create-users:config.json             Create users in DataHub
  create-groups:config.json            Create groups in DataHub  
  assign-ownership:config.json         Assign ownership to datasets

📈 VERSIONING OPERATIONS:
  version-update                       Update cloud and schema versions for ALL datasets (S-311→S-312, 1.0.0→2.0.0)
  datasets-summary-scan                Scan and display dataset summary with platform breakdown

🔍 EXTRACTION OPERATIONS (Direct to any format):
  extract:json-TYPE                    Extract metadata and save as JSON (comprehensive, schema, lineage, governance, properties, usage, quality, assertions, profiling, impact, metadata-diff)
  extract:excel-TYPE                   Extract metadata and export directly to Excel format
  extract:csv-TYPE                     Extract metadata and export directly to CSV format  
  extract:charts-TYPE                  Extract metadata and create visualizations directly

📊 EXAMPLES:
  # Ingest CSV files
  python framework_cli.py ingest:sample_configs_and_templates/ingestion/test_ingestion.json
  
  # Update all dataset versions
  python framework_cli.py version-update
  
  # Get comprehensive dataset analysis (backward compatibility - saves as JSON)
  python framework_cli.py extract:json-comprehensive
  
  # Extract data quality directly to Excel (no JSON intermediate step)
  python framework_cli.py extract:excel-quality
  
  # Extract governance to CSV format
  python framework_cli.py extract:csv-governance
  
  # Extract comprehensive data as JSON (traditional format)
  python framework_cli.py extract:json-comprehensive
  
  # Multiple extractions in different formats
  python framework_cli.py extract:csv-properties extract:excel-quality extract:charts-comprehensive

💡 NOTES:
  - Extraction operations use configs/global_settings.yaml automatically
  - Other operations require specific config files
  - Results are saved in the extracted_data/ directory in the format you specify
  - Operations can be chained together for comprehensive analysis
  - No intermediate JSON files needed - extract directly to your preferred format
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "operations",
        nargs='+',
        help="Operations to perform. Format: 'operation' or 'operation:config_path'"
    )
    
    parser.add_argument(
        "--version", "-v",
        action="version",
        version="🚀 DataHub Framework CLI v2.0.0\n📅 Built: 2025-08-21\n🏢 Enterprise Data Management Toolkit"
    )
    
    args = parser.parse_args()
    
    for op_config in args.operations:
        try:
            # Split the argument into the operation and its config path
            if ':' in op_config:
                operation, config_path = op_config.split(':', 1)
                logger.info(f"Executing operation '{operation}' with config '{config_path}'")
            else:
                operation = op_config
                config_path = None
                logger.info(f"Executing operation '{operation}'")
            
            if operation.lower() == "ingest":
                ingestion_controller.run_ingestion(config_path)
                
            elif operation.lower() == "add-lineage":
                lineage_controller.run_add_lineage(config_path)
                
            elif operation.lower() == "add-data-job-lineage":
                data_job_lineage_controller.run_add_data_job_lineage(config_path)
                
            elif operation.lower() == "enrich":
                enrichment_controller.run_enrichment(config_path)
                
            elif operation.lower() == "create-users":
                ownership_controller.run_create_users(config_path)
                
            elif operation.lower() == "create-groups":
                ownership_controller.run_create_groups(config_path)
                
            elif operation.lower() == "assign-ownership":
                ownership_controller.run_assign_ownership(config_path)
                
            elif operation.lower() == "version-update":
                # Update versions for all datasets in DataHub
                # Increments cloud version (S-311 -> S-312) and schema version (1.0.0 -> 2.0.0)
                # Updates the 'cloud_version' property in DataHub for each dataset
                version_controller.run_version_update()
                
            elif operation.lower() == "datasets-summary-scan":
                # Scan and display all datasets available in DataHub
                # Shows platform breakdown (CSV, AVRO, Hive, etc.) and dataset summary
                # Useful for checking what datasets exist before running version updates
                version_controller.run_dataset_scan()
            
            # New unified extraction operations (direct format output)
            elif operation.lower() == "extract" or operation.lower().startswith("extract-"):
                
                if operation.lower() == "extract":
                    # Handle new format: extract:format-type
                    if not config_path:
                        logger.error("Format required for extract operation")
                        logger.error("Format: extract:FORMAT-TYPE (e.g., extract:excel-quality, extract:csv-governance)")
                        continue
                    
                    format_type = config_path  # For extract operations, config_path is actually format-type
                    
                    # Parse format-type like "json-usage" or "excel-quality"
                    if "-" not in format_type:
                        logger.error(f"Invalid extraction format: {format_type}")
                        logger.error("Format: FORMAT-TYPE (e.g., excel-quality, csv-governance)")
                        continue
                    
                    parts = format_type.split("-", 1)  # Split only on first dash
                    output_format = parts[0]  # json, excel, csv, charts
                    extraction_type = parts[1]  # usage, quality, comprehensive, etc.
                    
                else:
                    # Handle backward compatibility: extract-TYPE (defaults to JSON) or extract-FORMAT-TYPE
                    operation_clean = operation.lower()
                    
                    # Split by dashes and identify format and type
                    parts = operation_clean.split("-")
                    if len(parts) < 3:  # Should be ["extract", "format", "type", ...]
                        # Handle backward compatibility (old format like "extract-comprehensive")
                        old_extraction_type = operation_clean.replace("extract-", "").replace("-", "_")
                        valid_types = ["comprehensive", "schema", "lineage", "governance", "properties", 
                                      "usage", "quality", "assertions", "profiling", "impact", "metadata_diff"]
                        if old_extraction_type.replace("-", "_") in [t.replace("-", "_") for t in valid_types]:
                            logger.info(f"🔄 Using backward compatibility mode - extracting {old_extraction_type} as JSON")
                            output_format = "json"
                            extraction_type = old_extraction_type
                        else:
                            logger.error(f"Invalid extraction format: {operation}")
                            logger.error("Format: extract:FORMAT-TYPE (e.g., extract:excel-quality, extract:csv-governance)")
                            continue
                    else:
                        # Old format: extract-FORMAT-TYPE (still supported)
                        output_format = parts[1]  # csv, excel, json, charts
                        extraction_type = "-".join(parts[2:])  # handle multi-part types like metadata-diff
                
                # Validate format
                valid_formats = ["json", "excel", "csv", "charts"]
                if output_format not in valid_formats:
                    logger.error(f"Unknown output format: {output_format}")
                    logger.error(f"Available formats: {', '.join(valid_formats)}")
                    continue
                
                # Validate extraction type
                valid_types = ["comprehensive", "schema", "lineage", "governance", "properties", 
                              "usage", "quality", "assertions", "profiling", "impact", "metadata_diff"]
                if extraction_type.replace("-", "_") not in [t.replace("-", "_") for t in valid_types]:
                    logger.error(f"Unknown extraction type: {extraction_type}")
                    logger.error(f"Available types: {', '.join(valid_types)}")
                    continue
                
                extraction_type_normalized = extraction_type.replace("-", "_")
                
                try:
                    config_manager = ConfigManager("configs/global_settings.yaml")
                    
                    # Create extraction config
                    import os
                    os.makedirs("extracted_data", exist_ok=True)
                    temp_json_file = f"extracted_data/temp_{extraction_type_normalized}_extraction.json"
                    extraction_config = {
                        "extraction_type": extraction_type_normalized,
                        "output_path": temp_json_file,
                        "datasets": "all"
                    }
                    
                    # Run extraction
                    result = ExtractionFactory.extract_with_config(extraction_config, config_manager)
                    
                    if not result.success:
                        logger.error(f"❌ {extraction_type.title()} extraction failed: {result.error_message}")
                        continue
                    
                    logger.info(f"📊 Extracted {result.extracted_count} datasets")
                    
                    # Now export to the requested format
                    if output_format == "json":
                        # Just rename the temp file to the final name
                        final_file = f"extracted_data/{extraction_type_normalized}_extraction.json"
                        os.rename(temp_json_file, final_file)
                        logger.info(f"✅ JSON extraction complete: {final_file}")
                    
                    elif output_format == "excel":
                        exporter = ExcelExporter()
                        if exporter.excel_available:
                            output_file = exporter.export(temp_json_file, f"extracted_data/{extraction_type_normalized}_report.xlsx")
                            logger.info(f"✅ Excel extraction complete: {output_file}")
                            # Clean up temp JSON
                            os.remove(temp_json_file)
                        else:
                            logger.error("❌ Excel export failed: pandas and openpyxl required")
                    
                    elif output_format == "csv":
                        exporter = CSVExporter()
                        output_file = exporter.export(temp_json_file, f"extracted_data/{extraction_type_normalized}_report.csv")
                        logger.info(f"✅ CSV extraction complete: {output_file}")
                        # Clean up temp JSON
                        os.remove(temp_json_file)
                    
                    elif output_format == "charts":
                        exporter = VisualizationExporter()
                        if exporter.viz_available:
                            chart_files = exporter.export(temp_json_file, f"extracted_data/{extraction_type_normalized}_charts")
                            logger.info(f"✅ Charts extraction complete: {len(chart_files)} charts created")
                            for chart in chart_files:
                                logger.info(f"   📊 {chart}")
                            # Clean up temp JSON
                            os.remove(temp_json_file)
                        else:
                            logger.error("❌ Charts export failed: matplotlib and seaborn required")
                        
                except Exception as e:
                    logger.error(f"Extraction failed: {e}")
                    # Clean up temp file if it exists
                    import os
                    if 'temp_json_file' in locals() and os.path.exists(temp_json_file):
                        os.remove(temp_json_file)
                
            else:
                logger.error(f"Unknown operation: {operation}")
                
        except ValueError:
            logger.error(f"Invalid operation format: '{op_config}'. Expected 'operation:config_path'.")
            
        except Exception as e:
            logger.error(f"A critical error occurred during operation '{op_config}': {e}", exc_info=True)

if __name__ == "__main__":
    main()