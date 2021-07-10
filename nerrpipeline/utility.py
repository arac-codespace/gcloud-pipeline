import os
# from google.cloud import storage


def create_subdirectories(directory: str) -> str:
    """
        Create a directory if it doesn't exist.
    """

    if not os.path.exists(directory):
        print("Subdirectory doesn't exist.  Creating it...")
        try:
            os.makedirs(directory)
            print("Success!")
        except Exception as err:
            print("Unexpected Error.")
            print(err)
            raise
        finally:
            return directory
    else:
        return directory


# def list_blobs_by_extension(
#     bucket: str,
#     extension: str,
#     prefix: Optional[str] = None
# ):
#     """
#         Function used to look for files in google storage
#         that ends with a certain extension.

#         No longer used, as Pipeline options are able to take care
#         of this functionality.
#     """
#     storage_client = storage.Client()
#     blobs = storage_client.list_blobs(bucket, prefix=prefix)
#     for blob in blobs:
#         if blob.name.endswith(extension):
#             yield blob
