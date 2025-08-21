#!/usr/bin/env python3
"""
Visualization Exporter
=====================

Exports extraction results as visualizations and charts.
"""

import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class VisualizationExporter:
    """Exports extraction results as visualizations."""
    
    def __init__(self):
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            self.plt = plt
            self.sns = sns
            self.viz_available = True
            
            # Set style
            self.sns.set_style("whitegrid")
            self.plt.style.use('seaborn-v0_8')
        except ImportError:
            logger.warning("matplotlib or seaborn not available. Visualization export disabled.")
            self.viz_available = False
    
    def export(self, json_file_path: str, output_dir: str = None) -> List[str]:
        """Export JSON extraction results as visualizations."""
        if not self.viz_available:
            raise ImportError("matplotlib and seaborn required for visualization export")
        
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        if not output_dir:
            output_dir = json_file_path.replace('.json', '_charts')
        
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        charts_created = []
        
        # Detect extraction type and create appropriate charts
        extraction_type = self._detect_extraction_type(data)
        
        if extraction_type == 'comprehensive':
            charts_created.extend(self._create_comprehensive_charts(data, output_dir))
        elif extraction_type == 'schema':
            charts_created.extend(self._create_schema_charts(data, output_dir))
        elif extraction_type == 'lineage':
            charts_created.extend(self._create_lineage_charts(data, output_dir))
        elif extraction_type == 'governance':
            charts_created.extend(self._create_governance_charts(data, output_dir))
        elif extraction_type == 'properties':
            charts_created.extend(self._create_properties_charts(data, output_dir))
        elif extraction_type == 'quality':
            charts_created.extend(self._create_quality_charts(data, output_dir))
        
        logger.info(f"Visualization export complete: {len(charts_created)} charts created in {output_dir}")
        return charts_created
    
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
    
    def _create_comprehensive_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for comprehensive extraction."""
        charts = []
        
        if 'datasets' in data:
            # Platform distribution pie chart
            platforms = {}
            for dataset in data['datasets']:
                platform = dataset.get('platform', 'unknown')
                platforms[platform] = platforms.get(platform, 0) + 1
            
            if platforms:
                chart_path = f"{output_dir}/platform_distribution.png"
                self.plt.figure(figsize=(10, 8))
                self.plt.pie(platforms.values(), labels=platforms.keys(), autopct='%1.1f%%')
                self.plt.title('Dataset Distribution by Platform')
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
    
    def _create_schema_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for schema analysis."""
        charts = []
        
        if 'type_analysis' in data:
            type_dist = data['type_analysis'].get('type_distribution', {})
            if type_dist:
                # Data type usage bar chart
                types = list(type_dist.keys())[:10]  # Top 10 types
                counts = [type_dist[t].get('count', 0) for t in types]
                
                chart_path = f"{output_dir}/data_type_usage.png"
                self.plt.figure(figsize=(12, 6))
                self.plt.bar(types, counts)
                self.plt.title('Top 10 Data Types Usage')
                self.plt.xlabel('Data Type')
                self.plt.ylabel('Usage Count')
                self.plt.xticks(rotation=45)
                self.plt.tight_layout()
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
    
    def _create_lineage_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for lineage analysis."""
        charts = []
        
        if 'impact_analysis' in data:
            high_impact = data['impact_analysis'].get('high_impact_datasets', [])
            if high_impact:
                # Impact score distribution
                names = [d.get('dataset_name', '')[:20] for d in high_impact[:10]]
                scores = [d.get('impact_score', 0) for d in high_impact[:10]]
                
                chart_path = f"{output_dir}/high_impact_datasets.png"
                self.plt.figure(figsize=(12, 8))
                self.plt.barh(names, scores)
                self.plt.title('Top 10 High Impact Datasets')
                self.plt.xlabel('Impact Score')
                self.plt.tight_layout()
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
    
    def _create_governance_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for governance analysis."""
        charts = []
        
        if 'governance_summary' in data:
            # Tag usage chart
            tag_analysis = data['governance_summary'].get('tag_analysis', {})
            most_common_tags = tag_analysis.get('most_common_tags', {})
            
            if most_common_tags:
                tags = list(most_common_tags.keys())[:10]
                counts = list(most_common_tags.values())[:10]
                
                chart_path = f"{output_dir}/tag_usage.png"
                self.plt.figure(figsize=(12, 6))
                self.plt.bar(tags, counts)
                self.plt.title('Top 10 Most Used Tags')
                self.plt.xlabel('Tag')
                self.plt.ylabel('Usage Count')
                self.plt.xticks(rotation=45)
                self.plt.tight_layout()
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
    
    def _create_properties_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for properties analysis."""
        charts = []
        
        if 'property_analysis' in data:
            prop_analysis = data['property_analysis'].get('custom_properties', {})
            if prop_analysis:
                # Property usage chart
                props = list(prop_analysis.keys())[:10]
                usage = [prop_analysis[p].get('usage_count', 0) for p in props]
                
                chart_path = f"{output_dir}/property_usage.png"
                self.plt.figure(figsize=(12, 6))
                self.plt.bar(props, usage)
                self.plt.title('Top 10 Custom Properties Usage')
                self.plt.xlabel('Property')
                self.plt.ylabel('Usage Count')
                self.plt.xticks(rotation=45)
                self.plt.tight_layout()
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
    
    def _create_quality_charts(self, data: Dict, output_dir: str) -> List[str]:
        """Create charts for quality analysis."""
        charts = []
        
        if 'quality_summary' in data:
            quality_dist = data['quality_summary'].get('quality_distribution', {})
            if quality_dist:
                # Quality distribution pie chart
                labels = [level.replace('_', ' ').title() for level in quality_dist.keys()]
                sizes = [info.get('count', 0) for info in quality_dist.values()]
                
                chart_path = f"{output_dir}/quality_distribution.png"
                self.plt.figure(figsize=(10, 8))
                self.plt.pie(sizes, labels=labels, autopct='%1.1f%%')
                self.plt.title('Dataset Quality Distribution')
                self.plt.savefig(chart_path, dpi=300, bbox_inches='tight')
                self.plt.close()
                charts.append(chart_path)
        
        return charts
