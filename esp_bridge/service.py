import win32serviceutil, win32service, win32event, subprocess, sys

class BridgeService(win32serviceutil.ServiceFramework):
    _svc_name_ = "SLH_ESP_Bridge"
    _svc_display_name_ = "SLH ESP32 Bridge"
    
    def SvcDoRun(self):
        self.process = subprocess.Popen(
            [sys.executable, r"D:\SLH_ECOSYSTEM\esp_bridge\bridge.py"]
        )
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
    
    def SvcStop(self):
        self.process.terminate()

if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(BridgeService)
