#!/usr/bin/env python3
"""
Excel Exporter
=============

Exports extraction results to Excel format for business users.
"""

import json
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class ExcelExporter:
    """Exports extraction results to Excel format."""
    
    def __init__(self):
        try:
            import pandas as pd
            import openpyxl
            self.pd = pd
            self.excel_available = True
        except ImportError:
            logger.warning("pandas or openpyxl not available. Excel export disabled.")
            self.excel_available = False
    
    def export(self, json_file_path: str, output_path: str = None) -> str:
        """Export JSON extraction results to Excel."""
        if not self.excel_available:
            raise ImportError("pandas and openpyxl required for Excel export")
        
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        if not output_path:
            output_path = json_file_path.replace('.json', '.xlsx')
        
        # Create Excel writer
        with self.pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Export datasets summary
            if 'datasets' in data:
                datasets_df = self._create_datasets_summary(data['datasets'])
                datasets_df.to_excel(writer, sheet_name='Datasets Summary', index=False)
            
            # Export specific analysis based on extraction type
            extraction_type = self._detect_extraction_type(data)
            
            if extraction_type == 'comprehensive':
                self._export_comprehensive(data, writer)
            elif extraction_type == 'schema':
                self._export_schema(data, writer)
            elif extraction_type == 'lineage':
                self._export_lineage(data, writer)
            elif extraction_type == 'governance':
                self._export_governance(data, writer)
            elif extraction_type == 'properties':
                self._export_properties(data, writer)
            elif extraction_type == 'quality':
                self._export_quality(data, writer)
        
        logger.info(f"Excel export complete: {output_path}")
        return output_path
    
    def _create_datasets_summary(self, datasets: List[Dict]) -> 'pd.DataFrame':
        """Create summary DataFrame of datasets."""
        summary_data = []
        
        for dataset in datasets:
            summary_data.append({
                'Dataset Name': dataset.get('name', 'Unknown'),
                'Platform': dataset.get('platform', 'Unknown'),
                'Environment': dataset.get('environment', 'Unknown'),
                'Description': dataset.get('description', '')[:100] + '...' if dataset.get('description') and len(dataset.get('description', '')) > 100 else dataset.get('description', ''),
                'Field Count': len(dataset.get('fields', [])),
                'Has Owners': bool(dataset.get('ownership', {}).get('owners')),
                'Has Tags': bool(dataset.get('governance', {}).get('tags')),
                'Has Lineage': bool(dataset.get('lineage', {}).get('upstream_datasets') or dataset.get('lineage', {}).get('downstream_datasets')),
                'Last Modified': dataset.get('operations', {}).get('last_modified', ''),
                'URN': dataset.get('urn', '')
            })
        
        return self.pd.DataFrame(summary_data)
    
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
    
    def _export_comprehensive(self, data: Dict, writer):
        """Export comprehensive extraction data."""
        if 'datasets' in data:
            # Create detailed datasets sheet
            detailed_data = []
            for dataset in data['datasets']:
                detailed_data.append({
                    'Name': dataset.get('name'),
                    'Platform': dataset.get('platform'),
                    'Environment': dataset.get('environment'),
                    'Field Count': len(dataset.get('fields', [])),
                    'Owner Count': len(dataset.get('ownership', {}).get('owners', [])),
                    'Tag Count': len(dataset.get('governance', {}).get('tags', [])),
                    'Upstream Count': len(dataset.get('lineage', {}).get('upstream_datasets', [])),
                    'Downstream Count': len(dataset.get('lineage', {}).get('downstream_datasets', []))
                })
            
            df = self.pd.DataFrame(detailed_data)
            df.to_excel(writer, sheet_name='Detailed Analysis', index=False)
    
    def _export_schema(self, data: Dict, writer):
        """Export schema extraction data."""
        if 'type_analysis' in data:
            type_dist = data['type_analysis'].get('type_distribution', {})
            type_data = []
            for type_name, info in type_dist.items():
                type_data.append({
                    'Data Type': type_name,
                    'Usage Count': info.get('count', 0),
                    'Platforms': ', '.join(info.get('platforms', [])),
                    'Native Types': ', '.join(info.get('native_types', []))
                })
            
            df = self.pd.DataFrame(type_data)
            df.to_excel(writer, sheet_name='Type Analysis', index=False)
    
    def _export_lineage(self, data: Dict, writer):
        """Export lineage extraction data."""
        if 'impact_analysis' in data:
            impact_data = data['impact_analysis'].get('high_impact_datasets', [])
            df = self.pd.DataFrame(impact_data)
            df.to_excel(writer, sheet_name='High Impact Datasets', index=False)
    
    def _export_governance(self, data: Dict, writer):
        """Export governance extraction data."""
        if 'governance_summary' in data:
            # Export tag analysis
            tag_analysis = data['governance_summary'].get('tag_analysis', {})
            most_common_tags = tag_analysis.get('most_common_tags', {})
            tag_data = [{'Tag': tag, 'Usage Count': count} for tag, count in most_common_tags.items()]
            
            df = self.pd.DataFrame(tag_data)
            df.to_excel(writer, sheet_name='Tag Analysis', index=False)
    
    def _export_properties(self, data: Dict, writer):
        """Export properties extraction data."""
        if 'property_analysis' in data:
            prop_analysis = data['property_analysis'].get('custom_properties', {})
            prop_data = []
            for prop_name, info in prop_analysis.items():
                prop_data.append({
                    'Property Name': prop_name,
                    'Usage Count': info.get('usage_count', 0),
                    'Unique Values': info.get('unique_values_count', 0),
                    'Usage Percentage': info.get('usage_percentage', 0),
                    'Platforms': ', '.join(info.get('platforms', []))
                })
            
            df = self.pd.DataFrame(prop_data)
            df.to_excel(writer, sheet_name='Property Analysis', index=False)
    
    def _export_quality(self, data: Dict, writer):
        """Export quality extraction data."""
        if 'quality_summary' in data:
            quality_dist = data['quality_summary'].get('quality_distribution', {})
            quality_data = []
            for level, info in quality_dist.items():
                quality_data.append({
                    'Quality Level': level.replace('_', ' ').title(),
                    'Dataset Count': info.get('count', 0),
                    'Percentage': info.get('percentage', 0)
                })
            
            df = self.pd.DataFrame(quality_data)
            df.to_excel(writer, sheet_name='Quality Distribution', index=False)
