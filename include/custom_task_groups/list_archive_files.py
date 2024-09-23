from airflow.decorators import task_group, task
from include.custom_operators.minio import MinIOHook, MinIOUploadOperator
import include.global_variables.global_variables as gv
import logging
import json

@task_group
def list_archive_files():
    
    @task
    def list_files():
        """List all files in the archive bucket."""
        logging.info("Starting to list files in the archive bucket.")
        minio_hook = MinIOHook()
        bucket_name = gv.ARCHIVE_BUCKET_NAME
        prefix = "archive/"
        
        # Manually handle recursive listing
        def list_all_objects(bucket_name, prefix):
            objects = minio_hook.list_objects(bucket_name, prefix)
            all_objects = []
            for obj in objects:
                all_objects.append({
                    "object_name": obj.object_name, 
                    "size": obj.size, 
                    "last_modified": obj.last_modified.isoformat() if obj.last_modified else None
                })
                if obj.object_name.endswith('/'):
                    all_objects.extend(list_all_objects(bucket_name, obj.object_name))
            return all_objects
        
        objects_list = list_all_objects(bucket_name, prefix)
        
        # Log the objects to inspect their structure
        for obj in objects_list:
            logging.info(f"Object found: {obj}")
        
        return objects_list

    @task
    def create_index_file(objects_list):
        """Create an index file in the archive bucket."""
        index_file_path = "/tmp/index.json"
        with open(index_file_path, 'w') as f:
            json.dump(objects_list, f)
        
        logging.info(f"Index file created at {index_file_path}.")
        return index_file_path

    @task
    def upload_index_file(index_file_path):
        """Upload the index file to MinIO."""
        logging.info(f"Starting to upload index file {index_file_path} to MinIO.")
        upload_task = MinIOUploadOperator(
            task_id="upload_index_file",
            bucket_name=gv.ARCHIVE_BUCKET_NAME,
            object_name="archive/index.json",
            file_path=index_file_path
        )
        upload_task.execute(context={})
        logging.info(f"Uploaded index.json to MinIO")

    files = list_files()
    index_file_path = create_index_file(files)
    upload_index_file(index_file_path)