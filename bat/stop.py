import os

if __name__ == '__main__':
    os.popen('taskkill /F /im nginx.exe')
    os.popen('taskkill /F /im python.exe')