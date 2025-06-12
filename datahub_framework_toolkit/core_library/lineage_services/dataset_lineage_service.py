# Service to manage dataset lineage

class DatasetLineageService:
    def add_lineage(self, upstream_urn, downstream_urn):
        print(f"Adding lineage: {upstream_urn} => {downstream_urn}")