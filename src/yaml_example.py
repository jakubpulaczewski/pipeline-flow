yaml_str = """
engine: [pandas, pyspark] ## use bridge pattern
transition : [data_storage, method_call, queue] ## use strategy pattern
plugins: [s3]

jobs:
    job1:
        extract:
            - name: Data_Extraction
            type: s3
            source: s3-bucket-name
            credentials:
                SECRET_KEY: encrypted_s3_key
                ACCESS_KEY: encrypted_access_key
        transform: []
        load:
            - name: Data_Load
            type: s3
            source: s3-bucket-name
            credentials:
                SECRET_KEY: encrypted_s3_key
                ACCESS_KEY: encrypted_access_key
    job2:
        needs: job1
        extract:
            - name: Data_Extraction2
                type: s3
                source: s3-bucket-name
                credentials:
                SECRET_KEY: encrypted_s3_key
                ACCESS_KEY: encrypted_access_key
        transform: []
        load:
            - name: Data_Load2
                type: s3
                source: s3-bucket-name
                credentials:
                SECRET_KEY: encrypted_s3_key
                ACCESS_KEY: encrypted_access_key
"""