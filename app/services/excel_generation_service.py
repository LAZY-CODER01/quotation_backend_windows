import os
import logging
from typing import Dict, Optional, List
import json
from datetime import datetime
import shutil

logger = logging.getLogger(__name__)


class ExcelGenerationService:
    """
    Service class for generating quotation Excel files.
    """
    
    def __init__(self, template_path: str = "sample/QuotationFormat.xlsx", output_dir: str = "generated"):
        """
        Initialize Excel generation service.
        
        Args:
            template_path (str): Path to the Excel template file
            output_dir (str): Directory to save generated files
        """
        self.template_path = template_path
        self.output_dir = output_dir
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"Excel generation service initialized")
        logger.info(f"Template: {template_path}")
        logger.info(f"Output directory: {output_dir}")
    
    
    def generate_quotation_excel(self, gmail_id: str, extraction_data: Dict, copy_only: bool = False) -> Optional[str]:
        """
        Generate a quotation Excel file from extraction data by copying the template, renaming, and editing.
        Uses ONLY win32com to preserve ALL Excel formatting perfectly.
        
        Args:
            gmail_id (str): Gmail message ID for unique filename
            extraction_data (Dict): Email extraction data from database
            copy_only (bool): If True, only copy the template without editing
        Returns:
            Optional[str]: Path to generated Excel file, None if failed
        """
        excel = None
        wb = None
        ws = None
        
        try:
            if not os.path.exists(self.template_path):
                raise FileNotFoundError(f"Template file not found: {self.template_path}")

            # Create unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quotation_{gmail_id}_{timestamp}.xlsx"
            output_path = os.path.join(self.output_dir, filename)

            # --- Step 1: Create exact copy (simulate Windows copy-paste) ---
            shutil.copy2(self.template_path, output_path)
            logger.info(f"Created exact copy: {output_path}")

            if copy_only:
                logger.info(f"Quotation Excel generated (copy only, perfect fidelity): {output_path}")
                return output_path

            # --- Step 2: Edit content using ONLY win32com ---
            extraction_result = extraction_data.get('extraction_result', {})
            if extraction_result:
                try:
                    from win32com.client import Dispatch
                    import pythoncom
                    
                    # Initialize COM
                    pythoncom.CoInitialize()
                    
                    # Open Excel application
                    excel = Dispatch("Excel.Application")
                    excel.Visible = False  # Run in background
                    excel.DisplayAlerts = False  # Suppress dialogs
                    
                    # Open the copied workbook
                    wb = excel.Workbooks.Open(os.path.abspath(output_path))
                    ws = wb.Worksheets(1)  # First worksheet
                    
                    # Fill the template
                    self._fill_quotation_template_win32(ws, extraction_data, extraction_result)
                    
                    # Save and close
                    wb.Save()
                    wb.Close(SaveChanges=True)
                    excel.Quit()
                    
                    logger.info("Template filled using win32com - ALL formatting preserved")
                    
                except ImportError:
                    logger.error("win32com not installed. Install with: pip install pywin32")
                    logger.error("Falling back to file copy only (no data insertion)")
                except Exception as e:
                    logger.error(f"Error using win32com: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    logger.error("Falling back to file copy only (no data insertion)")
                finally:
                    # Clean up COM objects
                    try:
                        if ws:
                            del ws
                        if wb:
                            del wb
                        if excel:
                            del excel
                        pythoncom.CoUninitialize()
                    except:
                        pass
            else:
                logger.info("No extraction_result data to fill. Only template copy performed.")

            logger.info(f"Quotation Excel generated: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error generating quotation Excel: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _fill_quotation_template_win32(self, ws, email_data: Dict, extraction_result: Dict):
        """
        Fill the Excel template with extracted data using win32com with rich text formatting.
        This preserves ALL formatting including headers, footers, watermarks.
        
        Args:
            ws: win32com Worksheet object
            email_data (Dict): Email metadata
            extraction_result (Dict): AI extraction result
        """
        try:
            # Client Information - Name in B2
            if 'to' in extraction_result:
                client_name = extraction_result.get('to', '')
                if client_name:
                    ws.Range('B2').Value = str(client_name)
            
            # Client Email - B3
            if 'email' in extraction_result:
                client_email = extraction_result.get('email', '')
                if client_email:
                    ws.Range('B3').Value = str(client_email)
            
            # Client Phone - B4
            if 'mobile' in extraction_result:
                client_phone = extraction_result.get('mobile', '')
                if client_phone:
                    ws.Range('B4').Value = str(client_phone)
            
            # Fill requirements/items starting from row 12, up to a maximum of 100 rows (row 12 to 111)
            requirements = extraction_result.get('Requirements', [])
            start_row = 12
            max_rows = 100
            current_row = start_row
            num_filled = 0
            if requirements:
                for idx, requirement in enumerate(requirements[:max_rows]):
                    req_row = start_row + idx
                    current_row = req_row
                    num_filled += 1
                    # Description with rich text formatting using GetCharacters
                    description_text = str(requirement.get('Description', ''))
                    # Build the complete text
                    part1 = "Your requirements:\n\n"
                    part2 = description_text + "\n\n"
                    part3 = "We OFFER:\n\n"
                    full_text = part1 + part2 + part3
                    # Set the full text in the cell first
                    cell = ws.Cells(req_row, 2)  # Column B
                    cell.Value = full_text
                    # Now format different parts with different colors using GetCharacters
                    # Part 1: "Your requirements:" - Purple, Bold, Underline
                    len_part1 = len(part1)
                    chars1 = cell.GetCharacters(1, len_part1)  # Start at position 1
                    chars1.Font.Color = 0x800080  # Purple (RGB in BGR format)
                    chars1.Font.Bold = True
                    chars1.Font.Underline = True
                    # Part 2: Description - Normal Black
                    len_part2 = len(part2)
                    chars2 = cell.GetCharacters(len_part1 + 1, len_part2)
                    chars2.Font.Color = 0x000000  # Black
                    chars2.Font.Bold = False
                    chars2.Font.Underline = False
                    # Part 3: "We OFFER:" - Red, Bold, Underline
                    len_part3 = len(part3)
                    chars3 = cell.GetCharacters(len_part1 + len_part2 + 1, len_part3)
                    chars3.Font.Color = 0x0000FF  # Red (RGB in BGR format: 0x00RRGGBB)
                    chars3.Font.Bold = True
                    chars3.Font.Underline = True
                    # Brand and model - Column C
                    brand_value = requirement.get("Brand and model", "")
                    if brand_value:
                        ws.Cells(req_row, 3).Value = str(brand_value)
                    # Quantity - Column F
                    qty_value = requirement.get("Quantity", "")
                    if qty_value:
                        ws.Cells(req_row, 6).Value = str(qty_value)
                    # Unit - Column G
                    unit_value = requirement.get("Unit", "")
                    if unit_value:
                        ws.Cells(req_row, 7).Value = str(unit_value)
                    # Unit price - Column H
                    unit_price_value = requirement.get("Unit price", "")
                    if unit_price_value:
                        ws.Cells(req_row, 8).Value = str(unit_price_value)
                    # Total Price - Column I
                    total_price_value = requirement.get("Total Price", "")
                    if total_price_value:
                        ws.Cells(req_row, 9).Value = str(total_price_value)

                # Delete leftover rows if less than max_rows filled
                if num_filled < max_rows:
                    delete_start = start_row + num_filled
                    delete_end = start_row + max_rows - 1
                    # Delete rows from delete_start to delete_end (inclusive)
                    # Use ws.Rows("{start}:{end}").Delete()
                    ws.Rows(f"{delete_start}:{delete_end}").Delete()



            logger.info("Template filled with extraction data using win32com with rich text")

        except Exception as e:
            logger.error(f"Error filling template with win32com: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def get_file_info(self, file_path: str) -> Optional[Dict]:
        """
        Get information about generated file.
        
        Args:
            file_path (str): Path to file
            
        Returns:
            Optional[Dict]: File information
        """
        try:
            if not os.path.exists(file_path):
                return None
            
            stat = os.stat(file_path)
            return {
                'filename': os.path.basename(file_path),
                'full_path': os.path.abspath(file_path),
                'size_bytes': stat.st_size,
                'size_mb': round(stat.st_size / (1024 * 1024), 2),
                'created': datetime.fromtimestamp(stat.st_ctime).isoformat(),
                'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
        except Exception as e:
            logger.error(f"Error getting file info: {str(e)}")
            return None



if __name__ == "__main__":
    """
    Test section for Excel generation service.
    Run this file directly to test Excel generation with hardcoded data.
    """
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test data
    test_data = {
        "extraction_result": {
            "Requirements": [
                {
            "Brand and model": "",
            "Description": "FLOOR STRIPPER 5 LITER",
            "Quantity": "40",
            "Total Price": "",
            "Unit": "Each",
            "Unit price": ""
          },
          {
            "Brand and model": "",
            "Description": "Hardware-Hard Brush w/Handle",
            "Quantity": "100",
            "Total Price": "",
            "Unit": "Numbers",
            "Unit price": ""
          },
          {
            "Brand and model": "",
            "Description": "Hardware-Soft Brush without Handle",
            "Quantity": "100",
            "Total Price": "",
            "Unit": "Numbers",
            "Unit price": ""
          },
          {
            "Brand and model": "",
            "Description": "TOILET BRUSH CLEANING WITH HOLDER",
            "Quantity": "80",
            "Total Price": "",
            "Unit": "Each",
            "Unit price": ""
          },
          {
            "Brand and model": "",
            "Description": "COCO BROOM WITH HANDLE",
            "Quantity": "100",
            "Total Price": "",
            "Unit": "Each",
            "Unit price": ""
          }
            ],
            "email": "sanatjha4@gmail.com",
            "mobile": "",
            "to": "Sanat Kumar Jha"
        },
        "extraction_status": "VALID",
        "gmail_id": "199b913ee7d694e4",
        "id": 3,
        "processed_at": "Mon, 06 Oct 2025 16:05:57 GMT",
        "received_at": "Mon, 06 Oct 2025 16:04:44 GMT",
        "sender": "Sanat Jha <sanatjha4@gmail.com>",
        "subject": "Inquiry for Screwdrivers",
        "updated_at": "Mon, 06 Oct 2025 16:24:48 GMT"
    }
    
    print("🧪 Testing Excel Generation Service")
    print("=" * 50)
    
    # Initialize the service
    excel_service = ExcelGenerationService(template_path="QuotationFormat.xlsx")
    
    # Test: Generate quotation Excel
    print("\n📊 Generating quotation Excel...")
    output_file = excel_service.generate_quotation_excel(
        gmail_id=test_data['gmail_id'],
        extraction_data=test_data
    )

    if output_file:
        print(f"✅ Excel generation successful!")
        print(f"   - File path: {output_file}")

        # Get file info
        print(f"\n📄 Getting file information...")
        file_info = excel_service.get_file_info(output_file)
        if file_info:
            print(f"✅ File info retrieved:")
            print(f"   - Filename: {file_info['filename']}")
            print(f"   - Size: {file_info['size_mb']} MB ({file_info['size_bytes']} bytes)")
            print(f"   - Created: {file_info['created']}")
        else:
            print(f"❌ Failed to get file information")

        print(f"\n🎯 Test completed successfully!")
        print(f"📁 You can find the generated file at: {os.path.abspath(output_file)}")
        #open the generated file automatically 
        os.startfile(os.path.abspath(output_file))

    else:
        print(f"❌ Excel generation failed!")

    print("\n" + "=" * 50)
