# Service for DQ assertions

class AssertionService:
    def assert_quality(self, dataset_urn, assertion):
        print(f"Asserting {assertion} on {dataset_urn}")