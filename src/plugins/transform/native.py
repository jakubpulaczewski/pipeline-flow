from common.type_def import ExtractedData, TransformedData

def clean_missing_values(id: str, key: str, fill_value=None):
    def inner(data: ExtractedData) -> list[dict]:
        """Replace missing values in a list of dictionaries for a given key."""
        return [
            {**item, key: item.get(key, fill_value)} for item in data
        ]
    return inner



# p