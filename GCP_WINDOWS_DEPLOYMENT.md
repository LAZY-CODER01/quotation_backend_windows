# Google Cloud Windows Server Deployment Guide

## Architecture Review
After deeply analyzing your `requirements.txt` and `new_excel_generation.py`, I identified a **critical dependency**: `pywin32` (`win32com.client.gencache.EnsureDispatch("Excel.Application")`).

1. **Why standard Docker/Linux won't work**: The `win32com` library directly manipulates the Windows COM API to open Microsoft Excel. **This requires a full Windows OS and a licensed installation of Microsoft Excel on the server.** Normal Linux Virtual Machines or Cloud Run deployments will instantly crash when trying to generate quotations.
2. **MotherDuck as Main Data Source**: Since your `duckdb_service.py` connects to MotherDuck (`md:snapquote_db`) when `MOTHERDUCK_TOKEN` is present, your database state is safe in the cloud. However, you still need reliable local storage for things like the `generated` Excel files and `tokens` (for Gmail OAuth).

---

## The Correct GCP Deployment Architecture: Windows Server VM

To run this backend in production securely and reliably, you must deploy a **Windows Server Virtual Machine** on Google Compute Engine (GCE). 

### Step 1: Provision the VM on Google Cloud
1. Go to the [Google Cloud Console](https://console.cloud.google.com/) -> Compute Engine -> VM Instances.
2. Click **Create Instance**.
3. **Name**: `quotesnap-windows-server`
4. **Machine Configuration**: `e2-medium` (2 vCPU, 4GB RAM) or higher (Excel COM automation is memory-heavy).
5. **Boot Disk**: Change the OS to **Windows Server 2022 Datacenter**. Set size to **50 GB SSD**.
6. **Firewall**: Allow HTTP (Port 80) and HTTPS (Port 443).
7. Click **Create**.

### Step 2: Install Required Software on the Windows VM
Once the VM is running, click the **RDP** button to connect to the Windows desktop.
You need to manually install the following:
1. **Python 3.10+**: Download and install from python.org (Check "Add Python to PATH").
2. **Microsoft Excel**: Install Microsoft Office / Excel (you will need to sign in with an Office 365 or standalone license). **This is non-negotiable for `win32com` to work.**
3. **Git**: Install Git for Windows to clone your repository.

### Step 3: Server Configuration & Deployment
Open PowerShell on your Windows Server as **Administrator** and clone your repository.
We have created an automated deployment script for you: `deploy_windows_gcp.ps1`.

Run it on the server:
```powershell
# Navigate to your cloned project folder
cd C:\path\to\quotationv3

# Run the deployment script
.\deploy_windows_gcp.ps1
```

### Step 4: Configure `Waitress` as a Windows Service
To ensure your backend stays alive when you disconnect from the remote desktop or when the server restarts, you should run it as a Windows Background Service using `NSSM` (Non-Sucking Service Manager).

1. Download NSSM: `curl -O https://nssm.cc/release/nssm-2.24.zip`
2. Extract it and open PowerShell as Administrator in the `win64` folder.
3. Install the service:
```powershell
.\nssm.exe install QuoteSnapBackend
```
4. A UI will open. Configure it as follows:
   * **Path**: `C:\path\to\quotationv3\venv\Scripts\python.exe`
   * **Arguments**: `run.py`
   * **Details -> Display Name**: QuoteSnap Backend
   * **Environment**: Add your production `.env` variables here (or rely on the `.env` file loaded by python-dotenv).
5. Click **Install Service**, then start it:
```powershell
Start-Service QuoteSnapBackend
```

---

## Firewall Rules (GCP)
If your `run.py` is configured to listen on port `8000`, you must open port 8000 in GCP.
1. In GCP Console, go to **VPC network -> Firewall**.
2. **Create Firewall Rule**.
3. Name: `allow-quotesnap-port`.
4. Target: All instances in the network.
5. IP Ranges: `0.0.0.0/0`.
6. Protocols & Ports: TCP -> `8000`.
7. Save.

You can now hit your public IP `http://<YOUR_VM_IP>:8000/api/health`.
