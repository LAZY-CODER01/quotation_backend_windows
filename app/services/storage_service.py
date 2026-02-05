import os
import cloudinary
import cloudinary.uploader
import cloudinary.api
import logging

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = [".xlsx"]

class StorageService:
    def __init__(self):
        if not all([
            os.getenv("CLOUDINARY_CLOUD_NAME"),
            os.getenv("CLOUDINARY_API_KEY"),
            os.getenv("CLOUDINARY_API_SECRET")
        ]):
            logger.warning("⚠️ Cloudinary credentials missing")

        cloudinary.config(
            cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
            api_key=os.getenv("CLOUDINARY_API_KEY"),
            api_secret=os.getenv("CLOUDINARY_API_SECRET"),
            secure=True
        )

    # 📤 Upload XLSX only
    def upload_excel(self, file_obj, filename, folder="excel_files"):
        try:
            ext = os.path.splitext(filename)[1].lower()

            if ext not in ALLOWED_EXTENSIONS:
                raise ValueError("Only .xlsx files are allowed")

            response = cloudinary.uploader.upload(
                file_obj,
                folder=folder,
                public_id=os.path.splitext(filename)[0],
                resource_type="raw"
            )

            return {
                "url": response["secure_url"],
                "public_id": response["public_id"]
            }

        except Exception as e:
            logger.exception("❌ Excel upload failed")
            return None

    # 🗑️ Delete file
    def delete_excel(self, public_id):
        try:
            result = cloudinary.uploader.destroy(
                public_id,
                resource_type="raw"
            )
            return result

        except Exception:
            logger.exception("❌ Delete failed")
            return None


    # 📤 Generic Upload (PDF, Images, etc.)
    def upload_file(self, file_obj, folder="uploads", public_id=None):
        try:
            # Determine resource type based on extension or let Cloudinary detect
            # For PDFs and images, "auto" usually works well, but "raw" is safer for non-media files if we treat them as such.
            # However, for previews, "auto" is better.
            
            upload_params = {
                "folder": folder,
                "resource_type": "auto"
            }
            
            if public_id:
                upload_params["public_id"] = public_id

            response = cloudinary.uploader.upload(file_obj, **upload_params)

            return response.get("secure_url")

        except Exception as e:
            logger.exception(f"❌ File upload failed: {e}")
            return None
