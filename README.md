# icfs


# Accounts
- Gmail: 
    ID : icloudfilesys@gmail.com
    PASS : icfs@2016

    ID_2 : icloudfilesys2@gmail.com
    PASS : icfs@2016

    ID_2 : icloudfilesys2@gmail.com
    PASS : icfs@2016
- Dropbox




Changes in  design
1) every account has root
Reason. when we init filesystem in a new PC we should be able to fetch hc_root file from any cloud

2) in create_root (file_object.py) addining one more condition
    a) check if root in local
    b) check if root in cloud
    c) otherwise, create in local and push

3) Moving the pushes to head layer
Reason: push head chunk and meta chunk in one push call

4) We have self.current_dir in filesystem to denote the current directory
Reason: every time we create a file we need to know the parent directory so that we can update the data chunks to include file list


5) In push (fileobject) if both primary and secondry fail we throw icfs exception which needs to be handled in filesystem










