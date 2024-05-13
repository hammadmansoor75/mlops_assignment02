from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Authenticate and create the PyDrive client.
gauth = GoogleAuth()
gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication.
drive = GoogleDrive(gauth)

# Choose the file to upload and the destination folder in Google Drive.
file_path = "test.txt"
folder_id = "1F3JbxtgLc0yTsye--78NdCl3cKgR8meE"  # You can find the folder ID in the URL of the folder.

# Upload the file.
file = drive.CreateFile({'parents': [{'id': folder_id}]})
file.SetContentFile(file_path)
file.Upload()
print("File uploaded successfully!")