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

                ws.Range("A2").Value = f"To: {meta_to}"
                ws.Range("A5").Value = f"Email: {meta_email}"
                ws.Range("A4").Value = f"Mobile: {meta_mobile}"
                ws.Range("E4").Value = f"Subject: {meta_subject}"
                ws.Range("E3").Value = f"Date: {datetime.now().strftime('%Y-%m-%d')}"
                # Make header bold
                for col in range(1, 5):
                    try:
                        ws.Cells(1, col).Font.Bold = True
                    except Exception:
                        pass
            except Exception:
                # don't fail the whole process if header insertion fails
                pass

            START_ROW = 12
            MAX_ROWS = 20

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
                ws.Cells(row, 6).Value = qty_val
                ws.Cells(row, 6).NumberFormat = "#,##0.00"

                # UNIT
                ws.Cells(row, 7).Value = item.get("Unit", "")

                # UNIT PRICE
                price_val = self._to_float(item.get("Unit price", 0))
                ws.Cells(row, 8).Value = price_val
                ws.Cells(row, 8).NumberFormat = "#,##0.00"

                # TOTAL (formula so Excel recalculates)
                ws.Cells(row, 9).Formula = f"=F{row}*H{row}"

                ws.Cells(row, 9).NumberFormat = "#,##0.00"

            # TOTALS (FIXED ROWS)
            last_row = START_ROW + len(requirements[:MAX_ROWS]) - 1
            TOTAL_ROW = last_row + 2
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1

            actual_rows = min(len(requirements), MAX_ROWS)
            if actual_rows >= 1:
                sum_start = START_ROW
                sum_end = START_ROW + actual_rows - 1
            else:
                sum_start = START_ROW
                sum_end = START_ROW

            ws.Cells(TOTAL_ROW, 9).Formula = f"=SUM(I{START_ROW}:I{last_row})"
            ws.Cells(TOTAL_ROW, 9).NumberFormat = "#,##0.00"
            ws.Cells(VAT_ROW, 9).Formula = f"=I{TOTAL_ROW}*0.05"
            ws.Cells(VAT_ROW, 9).NumberFormat = "#,##0.00"
            ws.Cells(GRAND_ROW, 9).Formula = f"=SUM(I{TOTAL_ROW}:I{VAT_ROW})"
            ws.Cells(GRAND_ROW, 9).NumberFormat = "#,##0.00"

        except Exception as e:
            logger.error(f"win32 error: {e}")

    def _fill_quotation_template_openpyxl(self, ws, email_data, extraction_result):
        from openpyxl.cell.rich_text import TextBlock, CellRichText
        from openpyxl.cell.text import InlineFont
        from openpyxl.styles import Font, Alignment

        requirements = extraction_result.get("Requirements", [])
        
        START_ROW = 12
        MAX_ROWS = 20

        # --- 1. FILL DATA ROWS ---
        for idx, item in enumerate(requirements[:MAX_ROWS]):
            row = START_ROW + idx
            try:
                # A. SL NO
                ws.cell(row=row, column=1).value = idx + 1

                # B. RICH TEXT DESCRIPTION
                desc_text = str(item.get("Description", "") or "N/A")
                offering_text = str(item.get("Company Offering", "") or "")

                header1 = TextBlock(InlineFont(b=True, u="single", color="800080"), "Client's requirements:\n")
                body1 = TextBlock(InlineFont(color="000000"), f"{desc_text}\n\n")
                header2 = TextBlock(InlineFont(b=True, u="single", color="FF0000"), "Company Offering:\n")
                body2 = TextBlock(InlineFont(color="000000"), f"{offering_text}")

                cell = ws.cell(row=row, column=2)
                cell.value = CellRichText([header1, body1, header2, body2])
                cell.alignment = Alignment(wrap_text=True, vertical="top")

                # C. ROW HEIGHT
                ws.row_dimensions[row].height = 140

                # D. QUANTITY & UNIT
                qty_val = self._to_float(item.get("Quantity", 0))
                ws.cell(row=row, column=6).value = qty_val
                ws.cell(row=row, column=6).number_format = "#,##0.00"
                ws.cell(row=row, column=7).value = item.get("Unit", "")

                # E. UNIT PRICE (Client View - Initially from extraction)
                # Note: If you want this to ALSO update based on CP/%, change value to f"=N{row}"
                price_val = self._to_float(item.get("Unit price", 0))
                ws.cell(row=row, column=8).value = price_val 
                ws.cell(row=row, column=8).number_format = "#,##0.00"

                # F. TOTAL PRICE (Client View)
                ws.cell(row=row, column=9).value = f"=F{row}*H{row}"
                ws.cell(row=row, column=9).number_format = "#,##0.00"
                
                # --- NEW CALCULATION LOGIC (Driven by CP & %) ---
                
                # K (11): CP (Cost Price) - Input
                ws.cell(row=row, column=11).value = 0.00
                ws.cell(row=row, column=11).number_format = "#,##0.00"

                # L (12): % (Margin Percentage) - Input
                # We set this to 0% initially. User types '20%' or '0.2'
                ws.cell(row=row, column=12).value = 0.00
                ws.cell(row=row, column=12).number_format = "0.00%"

                # N (14): SP (Selling Price) - Calculated
                # Formula: SP = CP * (1 + %)
                ws.cell(row=row, column=14).value = f"=K{row}*(1+L{row})"
                ws.cell(row=row, column=14).number_format = "#,##0.00"

                # M (13): Profit in AED - Calculated
                # Formula: Profit = (SP - CP) * Qty
                # We use the calculated SP (Col N) minus CP (Col K), times Qty (Col F)
                ws.cell(row=row, column=13).value = f"=(N{row}-K{row})*F{row}"
                ws.cell(row=row, column=13).number_format = "#,##0.00"

                # G. OTHER COLUMNS
                ws.cell(row=row, column=3).value = item.get("Brand and model", "")
                d_cell = ws.cell(row=row, column=5, value="Ex stock, subject to prior sales.")
                d_cell.font = Font(color="FF0000")
                d_cell.alignment = Alignment(wrap_text=True, vertical='center')

            except Exception as row_error:
                logger.error(f"Error processing row {row}: {row_error}")
                continue

        # --- 2. TOTALS & FOOTER ---
        try:
            actual_rows = len(requirements[:MAX_ROWS])
            last_data_row = START_ROW + actual_rows - 1
            if last_data_row < START_ROW: last_data_row = START_ROW

            TOTAL_ROW = last_data_row + 2
            VAT_ROW = TOTAL_ROW + 1
            GRAND_ROW = VAT_ROW + 1

            # Totals
            ws.cell(row=TOTAL_ROW, column=1).value = "Total Amount (AED)"
            ws.cell(row=TOTAL_ROW, column=9).value = f"=SUM(I{START_ROW}:I{last_data_row})"

            ws.cell(row=VAT_ROW, column=1).value = "VAT 5% (AED)"
            ws.cell(row=VAT_ROW, column=9).value = f"=I{TOTAL_ROW}*0.05"

            ws.cell(row=GRAND_ROW, column=1).value = "GRAND TOTAL AMOUNT (AED)"
            ws.cell(row=GRAND_ROW, column=9).value = f"=SUM(I{TOTAL_ROW}:I{VAT_ROW})"
            
            # Total Profit Sum
            ws.cell(row=TOTAL_ROW, column=13).value = f"=SUM(M{START_ROW}:M{last_data_row})"
            ws.cell(row=TOTAL_ROW, column=13).number_format = "#,##0.00"

            # Formatting
            for r in [TOTAL_ROW, VAT_ROW, GRAND_ROW]:
                ws.cell(row=r, column=1).font = Font(bold=True)
                ws.cell(row=r, column=9).number_format = "#,##0.00"
                ws.cell(row=r, column=9).font = Font(bold=True)

            # --- TERMS SECTION ---
            TERMS_START = GRAND_ROW + 2
            
            ws.cell(row=TERMS_START, column=1, value="TERMS:")
            ws.cell(row=TERMS_START, column=1).font = Font(bold=True, underline="single")

            terms = [
                ("Price:", ""),
                ("Delivery:", "Stated in Description Column Against Each Item."),
                ("Payment:", "30 Days Credit."),
                ("Validity:", "15 days from offer date.")
            ]

            for i, (key, value) in enumerate(terms):
                r = TERMS_START + 1 + i
                ws.cell(row=r, column=1, value=key)
                ws.cell(row=r, column=2, value=value)
                ws.cell(row=r, column=1).font = Font(bold=False)

            MSG_ROW = TERMS_START + len(terms) + 2
            ws.cell(row=MSG_ROW, column=1, value="Please revert for clarifications if any.")
            ws.cell(row=MSG_ROW + 1, column=1, value="Thank you for providing an opportunity to quote.")
            
            ws.cell(row=MSG_ROW + 3, column=1, value="Best Regards,")
            ws.cell(row=MSG_ROW + 3, column=1).font = Font(bold=True)

            ws.cell(row=MSG_ROW + 6, column=1, value="(This message has been electronically transmitted and does not require a signature).")
            ws.cell(row=MSG_ROW + 6, column=1).font = Font(italic=True, size=9)

        except Exception as e:
            logger.error(f"Error writing totals/terms: {e}")

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

