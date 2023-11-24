import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from pydantic import BaseModel
from typing import List
from fastapi import FastAPI, HTTPException, Body, Query
import io
from dotenv import load_dotenv
load_dotenv()
import json
# Create a service object to interact with the Drive API
print(os.getenv("GOOGLE_SHEETS_JSON_KEY_CONTENTS"))
SERVICE_ACCOUNT_JSON_PATH = json.loads(os.getenv("GOOGLE_SHEETS_JSON_KEY_CONTENTS"))

creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON_PATH, scopes=['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)
spreadsheet_service = build('sheets', 'v4', credentials=creds)

# Create FastAPI app
app = FastAPI()

# Pydantic model for GG sheet creation request
class CreateGoogleSheetRequest(BaseModel):
    new_spreadsheet_title: str
    permissions_email: str
    source_spreadsheet_id: str
    folder_id: str

# Pydantic model for folder creation request
class FolderCreateRequest(BaseModel):
    folder_name: str

# Pydantic model for finding a file by name request
class FindFileRequest(BaseModel):
    folder_id: str
    file_name: str

# Pydantic model for creating a Google Drive file request
class CreateDriveFileRequest(BaseModel):
    folder_id: str
    file_name: str
    file_content: str  # Content of the file as a string

class GetSheetNamesRequest(BaseModel):
    spreadsheet_id: str

class ReadWorksheetDataRequest(BaseModel):
    spreadsheet_id: str
    sheet_name: str


# Function to create a Google Drive folder
def create_folder(folder_name):
    folder_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
    return folder.get('id')

# Function to list files in a folder and retrieve their details
def list_files_in_folder(folder_id):
    results = drive_service.files().list(q=f"'{folder_id}' in parents", fields='files(id, name, createdTime)').execute()
    files = results.get('files', [])
    return files

# Function to find a file by its name and return its ID
def find_file_in_folder_id_by_name(folder_id, file_name):
    files = list_files_in_folder(folder_id)
    for file in files:
        if file['name'] == file_name:
            return file['id']
    return None

# Function to create a file in a specific folder
def create_file_in_folder(folder_id, file_name, file_content):
    file_content_bytes = file_content.encode('utf-8')
    media_filename = file_name
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
    }
    media_body = drive_service.files().create(
        body=file_metadata,
        media_body=io.BytesIO(file_content_bytes),
        fields='id'
    ).execute()
    return media_body.get('id')

# Function to create a copy of a Google Sheet with a new title and set writer permissions
def create_google_sheet(source_spreadsheet_id, new_spreadsheet_title, permissions_email, folder_id):
    # Create a copy of the source spreadsheet with the specified title
    copied_spreadsheet = {
        'name': new_spreadsheet_title,
        'parents': [folder_id]
    }
    new_spreadsheet = drive_service.files().copy(
        fileId=source_spreadsheet_id,
        body=copied_spreadsheet
    ).execute()

    # Get the file ID of the copied spreadsheet
    new_spreadsheet_id = new_spreadsheet['id']

    # Define permissions for write access to the specified email address
    permissions = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': permissions_email,
    }

    # Add permissions to the copied spreadsheet
    drive_service.permissions().create(fileId=new_spreadsheet_id, body=permissions).execute()

    # Get the web view link for the copied spreadsheet
    web_view_link = f'https://docs.google.com/spreadsheets/d/{new_spreadsheet_id}'

    return web_view_link

def list_files_and_folders():
    try:
        # List all files and folders in Google Drive
        results = drive_service.files().list(
            pageSize=1000,  # Adjust as needed for the number of files/folders you have
            fields="files(id, name, mimeType,createdTime, webViewLink)"
        ).execute()

        files = results.get('files', [])

        if not files:
            return "No files or folders found in Google Drive."
        else:
            output = []
            for item in files:
                file_info = {
                    "name": item['name'],
                    "id": item['id'],
                    "type": item['mimeType'],
                    "created": item['createdTime'],
                    "url" : item['webViewLink']
                }
                output.append(file_info)
            return output

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
def find_files_by_keyword(keyword):
    try:
        # List all files in Google Drive
        results = drive_service.files().list(
            q=f"name contains '{keyword}'",
            fields="files(id, name, mimeType,createdTime, webViewLink)"
        ).execute()

        files = results.get('files', [])

        # Create a list of dictionaries containing file ID and name
        file_list = [{"id": file['id'], "name": file['name'], "created": file['createdTime'], "url":file['webViewLink']} for file in files]
        return file_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# @app.post("/create_google_sheet")
# async def create_google_sheet(request_body: CreateGoogleSheetRequest):
  
#   # Example: Create a new spreadsheet by copying an existing one
#   # source_spreadsheet_id = '112hu6Mp1YqL3h7_GUSMzj42LtrABy7glxV5w3qW7UWU'
#   source_spreadsheet_id = request_body.source_spreadsheet_id
#   new_spreadsheet_title = request_body.new_spreadsheet_title
#   permissions_email = request_body.permissions_email
#   folder_id = request_body.folder_id  # Destination folder ID
#   try:
#       # Retrieve the metadata of the source file
#       source_file = drive_service.files().get(fileId=source_spreadsheet_id).execute()
      
#       # Create a copy of the source file
#       copied_file = {'name': new_spreadsheet_title}
#       new_file = drive_service.files().copy(
#           fileId=source_spreadsheet_id,
#           body=copied_file
#       ).execute()
#       new_file_id = new_file['id']
#       permissions = {
#           'type': 'user',
#           'role': 'writer',  # Change to 'reader' if you only need read access
#           'emailAddress': permissions_email,  # Replace with the email address of the user you want to grant access to
#       }
#       drive_service.permissions().create(fileId=new_file_id, body=permissions).execute()

#       print(f"Success! New file created:  https://docs.google.com/spreadsheets/d/{new_file_id}")
#       return f"https://docs.google.com/spreadsheets/d/{new_file_id}"
  
#   except Exception as e:
#       return str(e)

# Endpoint to create a Google Sheet with copy and permissions
@app.post("/create_google_sheet/", response_model=dict)
def create_google_sheet_endpoint(request_data: CreateGoogleSheetRequest):
    source_spreadsheet_id = request_data.source_spreadsheet_id
    new_spreadsheet_title = request_data.new_spreadsheet_title
    permissions_email = request_data.permissions_email
    folder_id = request_data.folder_id  # Destination folder ID

    web_view_link = create_google_sheet(source_spreadsheet_id, new_spreadsheet_title, permissions_email, folder_id)
    return {"message": f"Success! New Google Sheet created: {web_view_link}"}

# Endpoint to create a Google Drive folder
@app.post("/create_folder/", response_model=dict)
def create_google_drive_folder_endpoint(request_data: FolderCreateRequest):
    folder_name = request_data.folder_name
    folder_id = create_folder(folder_name)
    return {"message": f"Folder '{folder_name}' created with ID: {folder_id}"}

# Endpoint to list files in a folder
@app.get("/list_files/{folder_id}", response_model=List[dict])
def list_files_endpoint(folder_id: str):
    files = list_files_in_folder(folder_id)
    if not files:
        raise HTTPException(status_code=404, detail="Folder not found or empty")
    return files

# Endpoint to find a file by name in a folder
@app.post("/find_file_in_folder/", response_model=dict)
def find_file_in_folder_endpoint(request_data: FindFileRequest):
    folder_id = request_data.folder_id
    file_name = request_data.file_name
    file_id = find_file_in_folder_id_by_name(folder_id, file_name)
    if file_id:
        return {"message": f"File '{file_name}' found with ID: {file_id}"}
    else:
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found in the folder")

# Endpoint to create a file in a specific folder
@app.post("/create_file/", response_model=dict)
def create_file_endpoint(request_data: CreateDriveFileRequest):
    folder_id = request_data.folder_id
    file_name = request_data.file_name
    file_content = request_data.file_content

    file_id = create_file_in_folder(folder_id=folder_id, file_content=file_content, file_name=file_name)
    return {"message": f"File '{file_name}' created with ID: {file_id}"}

# Endpoint to list all files and folders in GG Drive
@app.get("/list_drive_files", response_model=list)
def list_drive_files():
    return list_files_and_folders()

# Endpoint to search file from keyword
@app.get("/find_files", response_model=list)
def find_files_by_keyword_endpoint(keyword: str = Query(..., description="Keyword to search for in file names")):
    file_list = find_files_by_keyword(keyword)

    if file_list:
        return file_list
    else:
        raise HTTPException(status_code=404, detail=f"No files found in Google Drive matching the keyword '{keyword}'")
    
@app.get("/get_sheet_names", response_model=list[str])
def get_sheet_names(request_body: GetSheetNamesRequest):
    spreadsheet_id = request_body.spreadsheet_id
    try:
        # Get a list of sheet names in the spreadsheet
        spreadsheet_metadata = spreadsheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet_metadata.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        return sheet_names
    except Exception as e:
        return {"error": str(e)}

@app.get("/read_worksheet_data")
def read_worksheet_data(request_body: ReadWorksheetDataRequest):
    spreadsheet_id = request_body.spreadsheet_id
    sheet_name = request_body.sheet_name
    try:
        # Read data from the specified sheet
        range_name = f"{sheet_name}"
        result = spreadsheet_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])
        
        if not values:
            return {"message": f"No data found in '{sheet_name}'."}
        else:
            return {"data": values}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    import json
    from fastapi.openapi.utils import get_openapi
    with open('openapi_gg_text.json', 'w') as f:
      json.dump(get_openapi(
          title="Vercel App Warper for GG API",
          version="1.0.0",
          summary="This is the warper loader using FastAPI and Vercel",
          openapi_version=app.openapi_version,
          description="Use this to create, read and update GG SpreadSheet using GG Sheet and GG Drive API",
          routes=app.routes,
          servers=[{"url" : "https://vercel-app-gg-api.vercel.app"}]
      ), f)


    # Run the FastAPI app using UVicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)