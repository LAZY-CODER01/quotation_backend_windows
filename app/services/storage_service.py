import os
import cloudinary
import cloudinary.uploader
import logging

logger = logging.getLogger(__name__)

class StorageService:
    def __init__(self):
        if not all([os.getenv("CLOUDINARY_CLOUD_NAME"), os.getenv("CLOUDINARY_API_KEY"), os.getenv("CLOUDINARY_API_SECRET")]):
            logger.warning("⚠️ Cloudinary credentials missing in environment variables")

        cloudinary.config(
            cloud_name=os.getenv("CLOUDINARY_CLOUD_NAME"),
            api_key=os.getenv("CLOUDINARY_API_KEY"),
            api_secret=os.getenv("CLOUDINARY_API_SECRET"),
            secure=True
        )

    def upload_file(self, file_obj, folder="quotations", public_id=None):
        try:
            response = cloudinary.uploader.upload(
                file_obj,
                folder=folder,
                public_id=public_id,
                resource_type="auto"
            )
            return response.get("secure_url")

        except Exception as e:
            # 🔥 THIS IS THE REAL ERROR YOU NEED
            logger.exception("❌ Cloudinary upload failed")
            return None
