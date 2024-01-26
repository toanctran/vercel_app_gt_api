import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
from pydantic import BaseModel
from typing import List
from fastapi import FastAPI, HTTPException, Body, Query
import io
import json



# Create a service object to interact with the Drive API
SERVICE_ACCOUNT_JSON_PATH = json.loads(os.getenv("GOOGLE_SHEETS_JSON_KEY_CONTENTS"))

creds = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON_PATH, scopes=['https://www.googleapis.com/auth/drive'])
drive_service = build('drive', 'v3', credentials=creds)
spreadsheet_service = build('sheets', 'v4', credentials=creds)

# Create FastAPI app
app = FastAPI()
@app.get("/")
async def root():
  return{"message":"Created by Tran Chi Toan - chitoantran@gmail.com"}

class CreateGoogleSheetRequest(BaseModel):
    new_spreadsheet_title: str
    permissions_email: str
    source_spreadsheet_id: str
    folder_id: str

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

    # # Get the file ID of the copied spreadsheet
    new_spreadsheet_id = new_spreadsheet['id']

    # # Define permissions for write access to the specified email address
    # permissions = {
    #     'type': 'user',
    #     'role': 'writer',
    #     'emailAddress': permissions_email,
    #     'sendNotificationEmails': False
    # }

    # # Add permissions to the copied spreadsheet
    # drive_service.permissions().create(fileId=new_spreadsheet_id, body=permissions, sendNotificationEmails = False,  fields='id').execute()

    # Get the web view link for the copied spreadsheet
    web_view_link = f'https://docs.google.com/spreadsheets/d/{new_spreadsheet_id}'

    return web_view_link

# Endpoint to create a Google Sheet with copy and permissions
@app.post("/create_google_sheet/")
def create_google_sheet_endpoint(request_data: CreateGoogleSheetRequest):
    source_spreadsheet_id = request_data.source_spreadsheet_id
    new_spreadsheet_title = request_data.new_spreadsheet_title
    permissions_email = request_data.permissions_email
    folder_id = request_data.folder_id  # Destination folder ID

    web_view_link = create_google_sheet(source_spreadsheet_id, new_spreadsheet_title, permissions_email, folder_id)
    return {"message": f"Success! New Google Sheet created: {web_view_link}"}

# Pydantic model for folder creation request
class CreateFolderRequest(BaseModel):
    parent_folder_id: str
    folder_name: str

# Function to create a Google Drive folder
def create_folder(parent_folder_id, folder_name):
    folder_metadata = {
        'name': folder_name,
        'parents':[parent_folder_id],
        'mimeType': 'application/vnd.google-apps.folder'
    }
    folder = drive_service.files().create(body=folder_metadata, fields='id').execute()

    web_view_link = f"https://drive.google.com/drive/u/0/folders/{folder.get('id')}"
    return folder.get('id'), web_view_link

# Endpoint to create a Google Drive folder
@app.post("/create_folder")
def create_google_drive_folder_endpoint(request_data: CreateFolderRequest):
    folder_name = request_data.folder_name
    parent_folder_id = request_data.parent_folder_id
    folder_id, folder_url = create_folder(parent_folder_id,folder_name)
    return {"message": f"Folder '{folder_name}' created with ID {folder_id} : {folder_url}"}

# Define Pydantic model for request parameters
class SearchFoldersRequest(BaseModel):
    keywords: str
    parent_folder_id: str

# Define Pydantic model for response format
class FolderInfo(BaseModel):
    folder_name: str
    folder_id: str
    created_time: str
    folder_url: str

# Route to search for folders
@app.post("/search_folder_in_folder/", response_model=list[FolderInfo])
async def search_folder_in_folder_endpoint(request_data: SearchFoldersRequest):
    keywords = request_data.keywords
    ParentFolderId = request_data.parent_folder_id
    
    # Search for folders with keywords in their name
    results = drive_service.files().list(q=f"name contains '{keywords}' and mimeType='application/vnd.google-apps.folder' and '{ParentFolderId}' in parents",
                                         fields="files(id, name, createdTime)").execute()
    
    folders_info = []
    for folder in results.get('files', []):
        folder_name = folder['name']
        folder_id = folder['id']
        created_time = folder['createdTime']
        folders_info.append(FolderInfo(folder_name=folder_name, folder_id=folder_id, created_time=created_time, folder_url=f"https://drive.google.com/drive/folders/{folder_id}"))
    
    return folders_info


# Function helpers for list all files and folders in Google Drive
def list_files_in_drive():
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
    
# Endpoint to list all files and folders in GG Drive
@app.get("/list_drive_files", response_model=list[dict])
def list_drive_files_endpoint():
    return list_files_in_drive()

# Function to list files in a folder_id and retrieve their details
def list_files_in_folder(folder_id):
    results = drive_service.files().list(q=f"'{folder_id}' in parents", fields='files(id, name, createdTime)').execute()
    files = results.get('files', [])
    return files

# Endpoint to list files in a specific folder_id
@app.get("/list_files_in_folder/{folder_id}", response_model=List[dict])
def list_files_in_folder_endpoint(folder_id: str):
    files = list_files_in_folder(folder_id)
    if not files:
        raise HTTPException(status_code=404, detail="Folder not found or empty")
    return files

# Function to list files in a folder_id and retrieve their details
def list_folders_in_folder(folder_id):
    query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder'"
    results = drive_service.files().list(q=query, fields='files(id, name, createdTime)').execute()
    files = results.get('files', [])
    return files

# Endpoint to list files in a specific folder_id
@app.get("/list_folders_in_folder/{folder_id}", response_model=List[dict])
def list_files_in_folder_endpoint(folder_id: str):
    files = list_folders_in_folder(folder_id)
    if not files:
        raise HTTPException(status_code=404, detail="Folder not found or empty")
    return files


# Function to find a file by its name and return its ID
def find_file_in_folder_id_by_name(folder_id, file_name):
    files = list_files_in_folder(folder_id)
    for file in files:
        if file['name'] == file_name:
            return file['id']
    return None

# Pydantic model for finding a file by name request
class SearchFileRequest(BaseModel):
    folder_id: str
    file_name: str

# Endpoint to find a file by name in a folder
@app.post("/search_file_in_folder")
def search_file_in_folder_endpoint(request_data: SearchFileRequest):
    folder_id = request_data.folder_id
    file_name = request_data.file_name
    file_id = find_file_in_folder_id_by_name(folder_id, file_name)
    if file_id:
        return {"message": f"File '{file_name}' found with ID: {file_id}"}
    else:
        raise HTTPException(status_code=404, detail=f"File '{file_name}' not found in the folder")
    
# Function helpers to search files with keywords in Google Drive
def find_files_by_keyword(keyword):
    try:
        # List all files in Google Drive
        results = drive_service.files().list(
            q=f"name contains '{keyword}' and trashed=false",
            fields="files(id, name, mimeType,createdTime, webViewLink)"
        ).execute()

        files = results.get('files', [])

        # Create a list of dictionaries containing file ID and name
        file_list = [{"id": file['id'], "name": file['name'], "created": file['createdTime'], "url":file['webViewLink']} for file in files]
        return file_list

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# # Endpoint to search file from keyword
# @app.get("/search_files", response_model=list[dict])
# def find_files_by_keyword_endpoint(keyword: str = Query(..., description="Keyword to search for in file names")):
#     file_list = find_files_by_keyword(keyword)

#     if file_list:
#         return file_list
#     else:
#         raise HTTPException(status_code=404, detail=f"No files found in Google Drive matching the keyword '{keyword}'")
    
# Define Pydantic model for request parameters to share a folder
class ShareFileRequest(BaseModel):
    file_id: str
    permission_email: str
    role: str

# Route to share a file   
@app.post("/share_file/")
async def share_file_endpoint(request_data: ShareFileRequest):
    file_id = request_data.file_id
    permission_email = request_data.permission_email
    role = request_data.role

    # Define the permission
    permission = {
        'type': 'user',
        'role': role,
        'emailAddress': permission_email,
        'sendNotificationEmails': False
    }
    
    try:
        # Share the folder with the specified email address
        drive_service.permissions().create(fileId=file_id, body=permission, sendNotificationEmails=False).execute()
        return {"message": f"File {file_id} shared with {permission_email} as a {role}."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Define Pydantic model for request parameters to share a folder
class ShareFolderRequest(BaseModel):
    folder_id: str
    permission_email: str
    role: str

# Route to share a folder
@app.post("/share_folder/")
async def share_folder_endpoint(request_data: ShareFolderRequest):
    folder_id = request_data.folder_id
    permission_email = request_data.permission_email
    role = request_data.role

    # Define the permission
    permission = {
        'type': 'user',
        'role': role,
        'emailAddress': permission_email,
        'sendNotificationEmails': False
    }
    
    try:
        # Share the folder with the specified email address
        drive_service.permissions().create(fileId=folder_id, body=permission, sendNotificationEmails=False).execute()
        return {"message": f"Folder {folder_id} shared with {permission_email} as a {role}."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

# Define Pydantic model for request parameters to update role permission to file and folder
class UpdatePermissionRoleRequest(BaseModel):
    file_id: str
    permission_email: str
    new_role: str

# Endpoint for updating file access permission
@app.post("/update_permission_role/")
async def update_permission_role_endpoint(request_data: UpdatePermissionRoleRequest):
    file_id = request_data.file_id
    permission_email = request_data.permission_email
    new_role = request_data.new_role
    
    try:
        # Get the current permissions for the file
        permissions = drive_service.permissions().list(fileId=file_id).execute()
        
        # Find the permission with the specified email address
        target_permission = None
        for permission in permissions.get('permissions', []):
            if permission['emailAddress'] == permission_email:
                target_permission = permission
                break
        
        if target_permission:
            # Update the role for the existing permission
            target_permission['role'] = new_role
            drive_service.permissions().update(fileId=file_id, permissionId=target_permission['id'], body=target_permission, sendNotificationEmails=False).execute()
            return {"message": f"Permission role for {permission_email} on file {file_id} updated to {new_role}."}
        else:
             # Define the permission
            permission = {
                'type': 'user',
                'role': new_role,
                'emailAddress': permission_email,
                'sendNotificationEmails': False
            }
            # Create a new permission with the requested role
            drive_service.permissions().create(fileId=file_id, body=permission, sendNotificationEmails=False).execute()
            return {"message": f"Created permission for {permission_email} on file {file_id} with role {new_role}."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    

# Pydantic model for get sheet name in spreadsheet request
class GetSheetNamesRequest(BaseModel):
    spreadsheet_id: str

@app.post("/get_sheet_names")
def get_sheet_names_endpoint(request_body: GetSheetNamesRequest):
    spreadsheet_id = request_body.spreadsheet_id
    try:
        # Get a list of sheet names in the spreadsheet
        spreadsheet_metadata = spreadsheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet_metadata.get('sheets', [])
        sheet_names = [sheet['properties']['title'] for sheet in sheets]
        return sheet_names
    except Exception as e:
        return {"error": str(e)}
    
#Create new Sheet
class AddNewSheetRequest(BaseModel):
    spreadsheet_id: str
    sheet_name: str

@app.post("/add_new_sheet")
def add_new_sheet(request_body: AddNewSheetRequest):
    spreadsheet_id = request_body.spreadsheet_id
    sheet_name = request_body.sheet_name

    batch_update_spreadsheet_request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": sheet_name
                    }
                }
            }
        ]
    }

    try:
        spreadsheet_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, 
            body=batch_update_spreadsheet_request_body
        ).execute()

        return {"message": f"Sheet '{sheet_name}' added successfully."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
# Pydantic model for get sheet rows data in spreadsheet request
class ReadWorksheetDataRequest(BaseModel):
    spreadsheet_id: str
    sheet_name: str

@app.post("/read_worksheet_rows")
def read_worksheet_row_endpoint(request_body: ReadWorksheetDataRequest):
    spreadsheet_id = request_body.spreadsheet_id
    sheet_name = request_body.sheet_name
    try:
        # Read data from the specified sheet
        range_name = f"{sheet_name}"
        result = spreadsheet_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])
        rows = {}
        for row, value in enumerate(values):
            rows[f'{row+1}'] = value
        if not values:
            return {"message": f"No data found in '{sheet_name}'."}
        else:
            return rows
    except Exception as e:
        return {"error": str(e)}
    


# Pydantic model for update content plan ro
class ContentPlanRowData(BaseModel):
    spreadsheet_id: str
    sheet_name: str
    video_number: str
    content_pillar: str
    video_title: str
    video_summary: str
    keywords: str
    video_description: str
    tags: str
    hashtags: str
    cta: str

# Function to find the first empty row in columns C to G starting from row 6
def find_empty_row_for_content_plan(spreadsheet_id, sheet_name):
    values = spreadsheet_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!B2:J").execute()
    data = values.get("values", [])
    if data:
        for i, row in enumerate(data):
            if all(cell == "" for cell in row):
                return i + 2  # Return the row number (2-based index)
    else:
        return 2
    return None

# Endpoint for adding new content plan row
@app.post("/add_content_plan_row/")
async def add_content_plan_row_endpoint(request_body: ContentPlanRowData):
    # Spreadsheet ID and Sheet name 
    spreadsheet_id = request_body.spreadsheet_id
    sheet_name = request_body.sheet_name # Replace with your Google Sheet ID

    # Prepare the data for the new row
    new_row_data = [[request_body.video_number, request_body.content_pillar, request_body.video_title, request_body.video_summary, request_body.keywords ,request_body.video_description, request_body.tags, request_body.hashtags, request_body.cta]]

    try:
        # Find the first empty row in columns B to J starting from row 2
        empty_row = find_empty_row_for_content_plan(spreadsheet_id,sheet_name)

        if empty_row is not None:
            # If an empty row is found, update it
            range_name = f"{sheet_name}!B{empty_row}:J{empty_row}"
        else:
            # If no empty row is found, add a new row
            empty_row = len(spreadsheet_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_name).execute().get("values", [])) + 1
            range_name = f"{sheet_name}!B{empty_row}:J{empty_row}"

        # Prepare the request body to add a new row or update the existing row
        request_body = {
            "values": new_row_data
        }

        # Make the API request to update or add the row
        response = spreadsheet_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            body=request_body
        ).execute()

        return {"message": f"Row {empty_row} updated/added successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# Define the data model for Spreadsheet Cell Update
class SpreadsheetCellUpdate(BaseModel):
    spreadsheet_id: str 
    sheet_name: str
    cell_column: str # This field represents the cell column location, e.g., "A", "B"
    cell_row: str  # This field represents the cell row location, e.g., "1", "2"
    content: str


# FastAPI endpoint to update the spreadsheet cell
@app.post("/update_spreadsheet_cell/")
async def update_spreadsheet_cell_endpoint(request_data: SpreadsheetCellUpdate):
    sheet_name = request_data.sheet_name
    spreadsheet_id = request_data.spreadsheet_id
    cell_row = request_data.cell_row
    cell_column = request_data.cell_column
    content = request_data.content
    

    try:
        cell = f"{cell_column}{cell_row}"
        values = [[content]]
        body = {
            'values': values
        }
        range_name = f"{sheet_name}!{cell}"
        value_input_option = "USER_ENTERED"
        result = spreadsheet_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption=value_input_option,
            body=body
        ).execute()
        return {"message": f"Cell {cell} in {sheet_name} updated successfully!"}
    except Exception as e:
        return {"error": str(e)}
    
# @app.delete("/delete_all_files")
# async def delete_all_files_endpoint(exclude_ids: list = Query(None)):
#     try:
#         # List all files in Google Drive
#         results = drive_service.files().list(fields="files(id)").execute()
#         files = results.get('files', [])

#         # Delete each file and folder that is not in the exclude_ids list
#         for file in files:
#             if file['id'] not in exclude_ids:
#                 drive_service.files().delete(fileId=file['id']).execute()

#         return {"message": "All files and folders except those in the exclude list have been deleted."}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# Define the Pydantic model for the request body
class SpreadsheetRequest(BaseModel):
    spreadsheet_id: str
    sheet_name: str
    row: int
    column_letter: str

# Function to check an empty cell
def is_cell_empty(spreadsheet_id: str, sheet_name: str, row: int, column_letter: str) -> bool:

    sheet = spreadsheet_service.spreadsheets()
    
    # Convert row and column to A1 notation
    # column_letter = chr(64 + column)
    range = f'{sheet_name}!{column_letter}{row}'

    # Get the cell value
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range).execute()
    values = result.get('values', [])

    # Check if the cell is empty
    return not values or not values[0]

# FastAPI endpoint
@app.post("/check_empty_cell")
def check_empty_cell_endpoint(request: SpreadsheetRequest):
    empty = is_cell_empty(request.spreadsheet_id, request.sheet_name, request.row, request.column_letter)
    return {"empty": empty}

@app.get("/get_spreadsheet_name/")
async def get_spreadsheet_name_endpoint(spreadsheet_url: str):
    import re
    # Extract the file ID from the URL
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", spreadsheet_url)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid Spreadsheet URL")

    file_id = match.group(1)

    # Use Google Drive API to get file details
    
    try:
        file = drive_service.files().get(fileId=file_id).execute()
        return {"file_name": file['name']}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    