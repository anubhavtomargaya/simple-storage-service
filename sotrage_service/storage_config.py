import os

STORAGE_CONFIG = {
    'location': os.getenv('GCS_LOCATION', 'asia-south1'),
    'default_storage_class': os.getenv('GCS_STORAGE_CLASS', 'STANDARD'),
    'labels': {
        'environment': os.getenv('GCS_ENV', 'development'),
        'project': os.getenv('GCS_PROJECT', 'dust-info')
    },
    'lifecycle_rules': [
        {
            'action': {
                'type': 'SetStorageClass',
                'storageClass': 'NEARLINE'
            },
            'condition': {
                'age': int(os.getenv('GCS_NEARLINE_AGE_DAYS', 30)),
                'matchesStorageClass': ['STANDARD']
            }
        },
        {
            'action': {
                'type': 'SetStorageClass',
                'storageClass': 'COLDLINE'
            },
            'condition': {
                'age': int(os.getenv('GCS_COLDLINE_AGE_DAYS', 90)),
                'matchesStorageClass': ['NEARLINE']
            }
        },
        {
            'action': {
                'type': 'Delete'
            },
            'condition': {
                'age': int(os.getenv('GCS_DELETE_AGE_DAYS', 365))
            }
        }
    ]
} 