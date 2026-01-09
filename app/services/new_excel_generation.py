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

    def __init__(
        self,
        template_path: str = "sample/QuotationFormat.xlsx",
        output_dir: str = "generated",
    ):
        """
        Initialize Excel generation service.

        Args:
            template_path (str): Path to the Excel template file
            output_dir (str): Directory to save generated files
        """
        self.template_path = template_path
        self.output_dir = output_dir

        # Forcefully delete the output directory if it exists
        if os.path.exists(output_dir):
            try:
                shutil.rmtree(output_dir)
                logger.info(f"Deleted existing output directory: {output_dir}")
            except Exception as e:
                logger.error(
                    f"Failed to delete output directory '{output_dir}': {str(e)}"
                )

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        logger.info(f"Excel generation service initialized")
        logger.info(f"Template: {template_path}")
        logger.info(f"Output directory: {output_dir}")

    def generate_quotation_excel(
        self, gmail_id: str, extraction_data: Dict, copy_only: bool = False
    ) -> Optional[str]:
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
                raise FileNotFoundError(
                    f"Template file not found: {self.template_path}"
                )

            # Create unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"quotation_{gmail_id}_{timestamp}.xlsx"
            output_path = os.path.join(self.output_dir, filename)

            # --- Step 1: Create exact copy (simulate Windows copy-paste) ---
            shutil.copy2(self.template_path, output_path)
            logger.info(f"Created exact copy: {output_path}")

            if copy_only:
                logger.info(
                    f"Quotation Excel generated (copy only, perfect fidelity): {output_path}"
                )
                return output_path

            # --- Step 2: Edit content using win32com (Windows) or openpyxl (Mac/Linux) ---
            extraction_result = extraction_data.get("extraction_result", {})
            logger.info(
                f"Extraction result type: {type(extraction_result)}, keys: {list(extraction_result.keys()) if isinstance(extraction_result, dict) else 'Not a dict'}"
            )
            if extraction_result:
                # Try win32com first (Windows)
                win32com_success = False
                try:

                    import platform

                    # Only use win32com on Windows
                    if platform.system() == "Windows":
                        # Initialize COM
                        import pythoncom

                        pythoncom.CoInitialize()

                    # Open Excel application
                    excel = Dispatch("Excel.Application")
                    excel.Visible = False  # Run in background
                    excel.DisplayAlerts = False  # Suppress dialogs

                    # Open the copied workbook
                    wb = excel.Workbooks.Open(os.path.abspath(output_path))
                    ws = wb.Worksheets(1)  # First worksheet

                    # Fill the template
                    self._fill_quotation_template_win32(
                        ws, extraction_data, extraction_result
                    )

                    # Save and close
                    wb.Save()
                    wb.Close(SaveChanges=True)
                    excel.Quit()

                    logger.info(
                        "Template filled using win32com - ALL formatting preserved"
                    )
                    win32com_success = True

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
                        logger.info("Not on Windows, skipping win32com")

                except ImportError:
                    logger.info("win32com not available (expected on Mac/Linux)")
                except Exception as e:
                    logger.warning(f"Error using win32com: {str(e)}")
                    import traceback

                    logger.debug(traceback.format_exc())

                # Fallback to openpyxl (Mac/Linux or if win32com failed)
                if not win32com_success:
                    logger.info(
                        f"win32com not successful (win32com_success={win32com_success}), using openpyxl fallback"
                    )
                    try:
                        from openpyxl import load_workbook

                        logger.info(
                            "Using openpyxl to fill template (Mac/Linux compatible)"
                        )
                        logger.info(
                            f"Extraction result for openpyxl: {json.dumps(extraction_result, indent=2)[:500]}"
                        )

                        # Load the workbook
                        wb = load_workbook(output_path)
                        ws = wb.active  # Get active worksheet

                        # Fill the template
                        self._fill_quotation_template_openpyxl(
                            ws, extraction_data, extraction_result
                        )

                        # Save the workbook
                        wb.save(output_path)
                        wb.close()

                        logger.info(
                            "Template filled using openpyxl - data inserted successfully"
                        )

                    except ImportError:
                        logger.error(
                            "openpyxl not installed. Install with: pip install openpyxl"
                        )
                        logger.error(
                            "Falling back to file copy only (no data insertion)"
                        )
                    except Exception as e:
                        logger.error(f"Error using openpyxl: {str(e)}")
                        import traceback

                        logger.error(traceback.format_exc())
                        logger.error(
                            "Falling back to file copy only (no data insertion)"
                        )
            else:
                logger.info(
                    "No extraction_result data to fill. Only template copy performed."
                )

            logger.info(f"Quotation Excel generated: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error generating quotation Excel: {str(e)}")
            import traceback

            logger.error(traceback.format_exc())
            return None

    def _fill_quotation_template_win32(self, ws, email_data, extraction_result):
        try:
            requirements = extraction_result.get("Requirements", [])

            # --- Header: write email metadata into first row ---
            try:
                meta_to = extraction_result.get("to", "") or (
                    email_data.get("extraction_result", {}) or {}
                ).get("to", "")
                meta_email = extraction_result.get("email", "") or (
                    email_data.get("extraction_result", {}) or {}
                ).get("email", "")
                meta_mobile = extraction_result.get("mobile", "") or (
                    email_data.get("extraction_result", {}) or {}
                ).get("mobile", "")
                meta_subject = email_data.get("subject", "") or extraction_result.get(
                    "subject", ""
                )

                ws.Cells(1, 1).Value = f"To: {meta_to}"
                ws.Cells(1, 2).Value = f"Email: {meta_email}"
                ws.Cells(1, 3).Value = f"Mobile: {meta_mobile}"
                ws.Cells(1, 4).Value = f"Subject: {meta_subject}"

                # Make header bold
                for col in range(1, 5):
                    try:
                        ws.Cells(1, col).Font.Bold = True
                    except Exception:
                        pass
            except Exception:
                # don't fail the whole process if header insertion fails
                pass

            START_ROW = 4
            MAX_ROWS = 6

            for idx, item in enumerate(requirements[:MAX_ROWS]):
                row = START_ROW + idx

                # SL NO
                ws.Cells(row, 1).Value = idx + 1

                # DESCRIPTION (Rich Text)
                client_req = item.get("Description", "")
                offering = item.get("Company Offering", "")

                text1 = "Client's requirements:\n"
                text2 = f"{client_req}\n\n"
                text3 = "Company Offering:\n"
                text4 = offering

                full_text = text1 + text2 + text3 + text4
                cell = ws.Cells(row, 2)
                cell.Value = full_text

                # formatting
                cell.GetCharacters(1, len(text1)).Font.Color = 0x800080
                cell.GetCharacters(1, len(text1)).Font.Bold = True

                cell.GetCharacters(
                    len(text1) + len(text2) + 1, len(text3)
                ).Font.Color = 0x0000FF
                cell.GetCharacters(
                    len(text1) + len(text2) + 1, len(text3)
                ).Font.Bold = True

                # QTY
                qty_val = self._to_float(item.get("Quantity", 0))
                ws.Cells(row, 3).Value = qty_val
                ws.Cells(row, 3).NumberFormat = "#,##0.00"

                # UNIT
                ws.Cells(row, 4).Value = item.get("Unit", "")

                # UNIT PRICE
                price_val = self._to_float(item.get("Unit price", 0))
                ws.Cells(row, 5).Value = price_val
                ws.Cells(row, 5).NumberFormat = "#,##0.00"

                # TOTAL (formula so Excel recalculates)
                ws.Cells(row, 6).Formula = f"=C{row}*E{row}"
                ws.Cells(row, 6).NumberFormat = "#,##0.00"

            # TOTALS (FIXED ROWS)
            TOTAL_ROW = START_ROW + MAX_ROWS + 1
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1

            actual_rows = min(len(requirements), MAX_ROWS)
            if actual_rows >= 1:
                sum_start = START_ROW
                sum_end = START_ROW + actual_rows - 1
            else:
                sum_start = START_ROW
                sum_end = START_ROW

            ws.Cells(TOTAL_ROW, 6).Formula = f"=SUM(F{sum_start}:F{sum_end})"
            ws.Cells(TOTAL_ROW, 6).NumberFormat = "#,##0.00"
            ws.Cells(VAT_ROW, 6).Formula = f"=F{TOTAL_ROW}*0.05"
            ws.Cells(VAT_ROW, 6).NumberFormat = "#,##0.00"
            ws.Cells(GRAND_ROW, 6).Formula = f"=F{TOTAL_ROW}*(1+0.05)"
            ws.Cells(GRAND_ROW, 6).NumberFormat = "#,##0.00"

        except Exception as e:
            logger.error(f"win32 error: {e}")

    def _fill_quotation_template_openpyxl(self, ws, email_data, extraction_result):
        try:
            from openpyxl.cell.rich_text import TextBlock, CellRichText
            from openpyxl.cell.text import InlineFont
            from openpyxl.styles import Font, Alignment

            requirements = extraction_result.get("Requirements", [])

            START_ROW = 4
            MAX_ROWS = 6

            for idx, item in enumerate(requirements[:MAX_ROWS]):
                row = START_ROW + idx

                # SL NO
                ws.cell(row=row, column=1).value = idx + 1

                # -------- RICH TEXT WITH PADDING --------
                desc_text = item.get("Description", "") or "N/A"
                offering_text = item.get("Company Offering", "") or ""

                header1 = TextBlock(
                    InlineFont(b=True, u="single", color="800080"),
                    "\nClient's requirements:\n",
                )

                body1 = TextBlock(InlineFont(color="000000"), f"{desc_text}\n\n")

                header2 = TextBlock(
                    InlineFont(b=True, u="single", color="FF0000"),
                    "Company Offering:\n",
                )

                body2 = TextBlock(InlineFont(color="000000"), f"{offering_text}\n")

                cell = ws.cell(row=row, column=2)

                cell.value = CellRichText([header1, body1, header2, body2])
                cell.alignment = Alignment(
                wrap_text=True,
                vertical="top"
            )

            # 🔒 FIXED HEIGHT FOR MERGED CELLS (CRITICAL)
                ws.row_dimensions[row].height = 140

                # -------- NUMBERS --------
                qty_val = self._to_float(item.get("Quantity", 0))
                price_val = self._to_float(item.get("Unit price", 0))

                ws.cell(row=row, column=3).value = qty_val
                ws.cell(row=row, column=3).number_format = "#,##0.00"

                ws.cell(row=row, column=4).value = item.get("Unit", "")

                ws.cell(row=row, column=5).value = price_val
                ws.cell(row=row, column=5).number_format = "#,##0.00"

                ws.cell(row=row, column=6).value = f"=C{row}*E{row}"
                ws.cell(row=row, column=6).number_format = "#,##0.00"

            # ================= TOTALS (DYNAMIC) =================

            last_item_row = START_ROW + len(requirements[:MAX_ROWS]) - 1

            TOTAL_ROW = last_item_row + 2
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1

            ws.cell(row=TOTAL_ROW, column=1).value = "Total"
            ws.cell(row=TOTAL_ROW, column=6).value = (
                f"=SUM(F{START_ROW}:F{last_item_row})"
            )

            ws.cell(row=VAT_ROW, column=1).value = "VAT (5%)"
            ws.cell(row=VAT_ROW, column=6).value = f"=F{TOTAL_ROW}*0.05"

            ws.cell(row=GRAND_ROW, column=1).value = "Grand Total"
            ws.cell(row=GRAND_ROW, column=6).value = f"=F{TOTAL_ROW}+F{VAT_ROW}"

            for r in [TOTAL_ROW, VAT_ROW, GRAND_ROW]:
                ws.cell(row=r, column=1).font = Font(bold=True)
                ws.cell(row=r, column=6).number_format = "#,##0.00"

        except Exception as e:
            logger.error(f"openpyxl rich text error: {e}")
            import traceback

            logger.error(traceback.format_exc())

    def _to_float(self, value) -> float:
        """
        Safely convert a value to float. Returns 0.0 on failure or for empty values.
        """
        try:
            if value is None:
                return 0.0
            s = str(value).strip().replace(",", "")
            return float(s) if s != "" else 0.0
        except Exception:
            return 0.0

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
                "filename": os.path.basename(file_path),
                "full_path": os.path.abspath(file_path),
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
                    "Unit price": "",
                },
                {
                    "Brand and model": "",
                    "Description": "Hardware-Hard Brush w/Handle",
                    "Quantity": "100",
                    "Total Price": "",
                    "Unit": "Numbers",
                    "Unit price": "",
                },
                {
                    "Brand and model": "",
                    "Description": "Hardware-Soft Brush without Handle",
                    "Quantity": "100",
                    "Total Price": "",
                    "Unit": "Numbers",
                    "Unit price": "",
                },
                {
                    "Brand and model": "",
                    "Description": "TOILET BRUSH CLEANING WITH HOLDER",
                    "Quantity": "80",
                    "Total Price": "",
                    "Unit": "Each",
                    "Unit price": "",
                },
                {
                    "Brand and model": "",
                    "Description": "COCO BROOM WITH HANDLE",
                    "Quantity": "100",
                    "Total Price": "",
                    "Unit": "Each",
                    "Unit price": "",
                },
            ],
            "email": "sanatjha4@gmail.com",
            "mobile": "",
            "to": "Sanat Kumar Jha",
        },
        "extraction_status": "VALID",
        "gmail_id": "199b913ee7d694e4",
        "id": 3,
        "processed_at": "Mon, 06 Oct 2025 16:05:57 GMT",
        "received_at": "Mon, 06 Oct 2025 16:04:44 GMT",
        "sender": "Sanat Jha <sanatjha4@gmail.com>",
        "subject": "Inquiry for Screwdrivers",
        "updated_at": "Mon, 06 Oct 2025 16:24:48 GMT",
    }

    print("🧪 Testing Excel Generation Service")
    print("=" * 50)

    # Initialize the service
    excel_service = ExcelGenerationService(template_path="QuotationFormat.xlsx")

    # Test: Generate quotation Excel
    print("\n📊 Generating quotation Excel...")
    output_file = excel_service.generate_quotation_excel(
        gmail_id=test_data["gmail_id"], extraction_data=test_data
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
            print(
                f"   - Size: {file_info['size_mb']} MB ({file_info['size_bytes']} bytes)"
            )
            print(f"   - Created: {file_info['created']}")
        else:
            print(f"❌ Failed to get file information")

        print(f"\n🎯 Test completed successfully!")
        print(f"📁 You can find the generated file at: {os.path.abspath(output_file)}")
        # open the generated file automatically
        os.startfile(os.path.abspath(output_file))

    else:
        print(f"❌ Excel generation failed!")

    print("\n" + "=" * 50)
