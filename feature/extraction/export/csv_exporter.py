#!/usr/bin/env python3
"""
CSV Exporter
===========

Exports extraction results to CSV format for analysis and reporting.
"""

import json
import csv
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVExporter:
    """Exports extraction results to CSV format."""
    
    def export(self, json_file_path: str, output_path: str = None) -> str:
        """Export JSON extraction results to CSV."""
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        if not output_path:
            output_path = json_file_path.replace('.json', '.csv')
        
        # Create main datasets CSV
        datasets = data.get('datasets', [])
        if datasets:
            self._export_datasets_csv(datasets, output_path)
        
        # Create analysis-specific CSVs
        extraction_type = self._detect_extraction_type(data)
        base_path = output_path.replace('.csv', '')
        
        if extraction_type == 'comprehensive':
            self._export_comprehensive_csv(data, base_path)
        elif extraction_type == 'schema':
            self._export_schema_csv(data, base_path)
        elif extraction_type == 'lineage':
            self._export_lineage_csv(data, base_path)
        elif extraction_type == 'governance':
            self._export_governance_csv(data, base_path)
        elif extraction_type == 'properties':
            self._export_properties_csv(data, base_path)
        elif extraction_type == 'quality':
            self._export_quality_csv(data, base_path)
        
        logger.info(f"CSV export complete: {output_path}")
        return output_path
    
    def _export_datasets_csv(self, datasets: List[Dict], output_path: str):
        """Export main datasets summary to CSV."""
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'dataset_name', 'platform', 'environment', 'description',
                'field_count', 'has_owners', 'has_tags', 'has_lineage',
                'last_modified', 'urn'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for dataset in datasets:
                writer.writerow({
                    'dataset_name': dataset.get('name', ''),
                    'platform': dataset.get('platform', ''),
                    'environment': dataset.get('environment', ''),
                    'description': dataset.get('description', '')[:200] if dataset.get('description') else '',
                    'field_count': len(dataset.get('fields', [])),
                    'has_owners': bool(dataset.get('ownership', {}).get('owners')),
                    'has_tags': bool(dataset.get('governance', {}).get('tags')),
                    'has_lineage': bool(dataset.get('lineage', {}).get('upstream_datasets') or 
                                      dataset.get('lineage', {}).get('downstream_datasets')),
                    'last_modified': dataset.get('operations', {}).get('last_modified', ''),
                    'urn': dataset.get('urn', '')
                })
    
    def _detect_extraction_type(self, data: Dict) -> str:
        """Detect the type of extraction from the data structure."""
        if 'lineage_graph' in data:
            return 'lineage'
        elif 'governance_summary' in data:
            return 'governance'
        elif 'property_analysis' in data:
            return 'properties'
        elif 'quality_summary' in data:
            return 'quality'
        elif 'schemas' in data:
            return 'schema'
        elif 'datasets' in data:
            return 'comprehensive'
        else:
            return 'unknown'
    
    def _export_comprehensive_csv(self, data: Dict, base_path: str):
        """Export comprehensive extraction data to multiple CSVs."""
        # Export lineage relationships
        if 'datasets' in data:
            lineage_path = f"{base_path}_lineage.csv"
            with open(lineage_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['dataset_name', 'upstream_count', 'downstream_count', 'total_connections']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for dataset in data['datasets']:
                    lineage = dataset.get('lineage', {})
                    upstream_count = len(lineage.get('upstream_datasets', []))
                    downstream_count = len(lineage.get('downstream_datasets', []))
                    
                    writer.writerow({
                        'dataset_name': dataset.get('name', ''),
                        'upstream_count': upstream_count,
                        'downstream_count': downstream_count,
                        'total_connections': upstream_count + downstream_count
                    })
    
    def _export_schema_csv(self, data: Dict, base_path: str):
        """Export schema analysis to CSV."""
        if 'type_analysis' in data:
            type_path = f"{base_path}_types.csv"
            type_dist = data['type_analysis'].get('type_distribution', {})
            
            with open(type_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['data_type', 'usage_count', 'platforms', 'native_types']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for type_name, info in type_dist.items():
                    writer.writerow({
                        'data_type': type_name,
                        'usage_count': info.get('count', 0),
                        'platforms': ', '.join(info.get('platforms', [])),
                        'native_types': ', '.join(info.get('native_types', []))
                    })
    
    def _export_lineage_csv(self, data: Dict, base_path: str):
        """Export lineage analysis to CSV."""
        if 'impact_analysis' in data:
            impact_path = f"{base_path}_impact.csv"
            impact_data = data['impact_analysis'].get('high_impact_datasets', [])
            
            if impact_data:
                with open(impact_path, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = list(impact_data[0].keys()) if impact_data else []
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(impact_data)
    
    def _export_governance_csv(self, data: Dict, base_path: str):
        """Export governance analysis to CSV."""
        if 'governance_summary' in data:
            # Export tag analysis
            tag_path = f"{base_path}_tags.csv"
            tag_analysis = data['governance_summary'].get('tag_analysis', {})
            most_common_tags = tag_analysis.get('most_common_tags', {})
            
            with open(tag_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['tag_name', 'usage_count']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for tag, count in most_common_tags.items():
                    writer.writerow({'tag_name': tag, 'usage_count': count})
    
    def _export_properties_csv(self, data: Dict, base_path: str):
        """Export properties analysis to CSV."""
        if 'property_analysis' in data:
            prop_path = f"{base_path}_properties.csv"
            prop_analysis = data['property_analysis'].get('custom_properties', {})
            
            with open(prop_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['property_name', 'usage_count', 'unique_values', 'usage_percentage', 'platforms']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for prop_name, info in prop_analysis.items():
                    writer.writerow({
                        'property_name': prop_name,
                        'usage_count': info.get('usage_count', 0),
                        'unique_values': info.get('unique_values_count', 0),
                        'usage_percentage': info.get('usage_percentage', 0),
                        'platforms': ', '.join(info.get('platforms', []))
                    })
    
    def _export_quality_csv(self, data: Dict, base_path: str):
        """Export quality analysis to CSV."""
        if 'quality_summary' in data:
            quality_path = f"{base_path}_quality.csv"
            quality_dist = data['quality_summary'].get('quality_distribution', {})
            
            with open(quality_path, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['quality_level', 'dataset_count', 'percentage']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for level, info in quality_dist.items():
                    writer.writerow({
                        'quality_level': level.replace('_', ' ').title(),
                        'dataset_count': info.get('count', 0),
                        'percentage': info.get('percentage', 0)
                    })
