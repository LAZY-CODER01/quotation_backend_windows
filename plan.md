# Project Plan – SnapQuote (Gmail Quotation Automation Agent)

## 1. Project Scope

Party A will develop **SnapQuote**, a web-based application to automate quotation handling from Gmail.
The application will be delivered in **two phases**:

* **Phase 1 (MVP):** Email scraping, quotation data extraction, and Excel formatting.
* **Phase 2:** Database matching using vector search, intelligent quotation generation, and supplier communication.

---

## 2. Deliverables and Timeline

### **Phase 1 Deliverables – Email Monitoring & Excel Quotation Preparation**

**2.1. Inbound Email Monitoring**

* Configure and monitor a new designated **Gmail account** (to be provided by Party B).
* System will continuously track incoming emails using the **Gmail API**.
* Authentication via **Google OAuth 2.0**.

**2.2. Email Data Extraction**

* Each email will be parsed to extract structured details (product names, quantities, client details, deadlines, etc.) in **JSON format**.
* Attachments (Excel, PDF, etc.) will also be parsed if needed.

**2.3. Excel Quotation Formatting**

* Extracted data mapped into the company’s **standard Excel quotation template**.
* Template fields include:

  * Serial Number
  * Customer Requirement
  * Quantity
  * Other mandatory fields (as required by Party B).
* Excel created using **Python libraries** (e.g., `openpyxl`, `pandas`).

**2.4. Dashboard & Metrics**

* A **dashboard** to display real-time metrics:

  * Total emails received
  * Total quotation requests processed
  * Total Excel quotations generated
* Built with **Tailwind CSS, HTML, JS** for a responsive UI.

**2.5. Documentation**

* Provide documentation covering setup, workflow, and usage.

---

### **Phase 2 Deliverables – Database Matching & Intelligent Quotation Generation**

**2.6. Quotation Database Creation**

* Build a **vector database** to store embeddings of previously generated quotations.
* Allows semantic search of historical quotations using **OpenAI embeddings**.

**2.7. Matching Engine**

* New requests are automatically compared against stored embeddings:

  * If match found → reuse/refer historical product and pricing information.
  * If no match → trigger supplier communication workflow.

**2.8. Supplier Communication (WhatsApp API)**

* If no historical match exists, the system sends supplier requests via **WhatsApp API (Twilio/Meta Cloud API)**.
* Supplier responses logged in the database for future use.
* Manual processing by Party B’s team, but data is retained for automation learning.

**2.9. Excel Quotation Integration**

* Populate matched/unmatched quotation requests into the Excel template.
* Auto-flag unmatched quotations for manual completion.

**2.10. Dashboard Enhancements**

* Additional dashboard features:

  * Quotations matched from history (via vector search)
  * Quotations flagged as unmatched
  * Success rate of quotation matches
  * Auto-generated PDF/Excel quotation downloads

**2.11. Documentation**

* Comprehensive documentation for Phase 2, covering vector DB usage, supplier communication workflows, and scalability.

---

## 3. Technical Requirements

**Frontend:**

* **HTML, Tailwind CSS, JS** – Login & Dashboard UI

**Backend:**

* **Flask (Python)** – API server, Gmail monitoring, AI integration

**Database:**

* **SQLite** – lightweight, easy deployment
* **Vector Database (e.g., Pinecone, Weaviate, FAISS, or Qdrant)** – for semantic search of historical quotations
* Tables:

  * `Emails` – stores raw metadata
  * `QuotationRequests` – structured extracted details
  * `QuotationHistory` – previous quotations, PDF/Excel references + embeddings
  * `Suppliers` – supplier contacts and categories
  * `Logs` – auditing and action logs

**APIs:**

* Gmail API (email monitoring)
* OpenAI GPT API + Embeddings API (data extraction + vector matching)
* WhatsApp API (supplier messaging)

**PDF/Excel Tools:**

* **Excel:** `openpyxl`, `pandas`
* **PDF:** `ReportLab`, `WeasyPrint`

**Security:**

* OAuth 2.0 for Gmail access
* HTTPS for all traffic
* Secure handling of supplier contact data and API tokens

---

## 4. Metrics & Dashboard Data Points

* Total emails received
* Total quotation requests processed
* Total Excel/PDF quotations auto-generated
* Quotations matched via vector search
* Quotations flagged as unmatched
* WhatsApp supplier requests sent
* Pending supplier responses
* Historical match success rate
* Time of last email processed

---

## 5. Deliverables Summary

**Phase 1**
✅ Gmail monitoring system
✅ Extraction of structured quotation details
✅ Excel template population
✅ Initial dashboard (metrics + visualization)
✅ Documentation

**Phase 2**
✅ Vector database for quotation matching
✅ AI-powered semantic search for past data
✅ Auto-generation of quotations (Excel/PDF)
✅ WhatsApp supplier communication
✅ Enhanced dashboard with historical insights
✅ Documentation

---
