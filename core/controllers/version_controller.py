from core.common.config_manager import ConfigManager
from feature.versioning.dataset_scanner import DatasetScanner
from feature.versioning.version_service import VersionManager


def run_version_update():
    """
    Update versions for all datasets in DataHub.
    
    This function:
    1. Scans all datasets from DataHub
    2. Increments cloud and schema versions
    3. Updates DataHub properties
    """
    print("🚀 Starting DataHub Version Update")
    print("=" * 50)
    
    # Initialize components using existing framework pattern
    config_manager = ConfigManager("configs/global_settings.yaml")
    
    # Initialize services
    dataset_scanner = DatasetScanner(config_manager)
    version_manager = VersionManager(config_manager)
    
    # Step 1: Scan datasets
    datasets = dataset_scanner.scan_all_datasets()
    
    if not datasets:
        print("❌ No datasets found in DataHub")
        return
    
    # Show platform breakdown
    platform_summary = dataset_scanner.get_platform_summary(datasets)
    print("\n📊 Dataset breakdown by platform:")
    for platform, count in sorted(platform_summary.items()):
        print(f"   {platform}: {count} datasets")
    
    # Step 2: Update versions
    dataset_urns = [dataset.urn for dataset in datasets]
    update_results = version_manager.bulk_update_versions(dataset_urns)
    
    # Step 3: Summary
    success_count = sum(1 for r in update_results if r.success)
    failure_count = len(update_results) - success_count
    
    print("\n🎉" + "=" * 50 + "🎉")
    print("           VERSION UPDATE COMPLETE")
    print("🎉" + "=" * 50 + "🎉")
    print(f"📊 Total datasets: {len(datasets)}")
    print(f"✅ Successfully updated: {success_count}")
    print(f"❌ Failed: {failure_count}")
    print("🎉" + "=" * 50 + "🎉")


def run_dataset_scan():
    """
    Scan and display all datasets in DataHub.
    
    This function:
    1. Discovers all datasets from DataHub
    2. Shows platform breakdown  
    3. Displays summary information
    """
    print("🔍 Starting DataHub Dataset Scan")
    print("=" * 50)
    
    # Initialize components
    config_manager = ConfigManager("configs/global_settings.yaml")
    dataset_scanner = DatasetScanner(config_manager)
    
    # Scan datasets
    datasets = dataset_scanner.scan_all_datasets()
    
    if not datasets:
        print("❌ No datasets found in DataHub")
        return
    
    # Show results
    platform_summary = dataset_scanner.get_platform_summary(datasets)
    
    print("\n📊 Dataset Summary:")
    print(f"Total datasets found: {len(datasets)}")
    
    print("\nPlatform breakdown:")
    for platform, count in sorted(platform_summary.items()):
        print(f"  {platform}: {count} datasets")
    
    print("\nSample datasets:")
    for i, dataset in enumerate(datasets[:5], 1):
        print(f"  {i}. {dataset.name} ({dataset.platform})")
    
    if len(datasets) > 5:
        print(f"  ... and {len(datasets) - 5} more datasets")
    
    print("\n✅ Dataset scan complete")