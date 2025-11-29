/*******************************
 CONFIG
*******************************/
const SHEET_ID = '...';
const PROCESSED_LABEL_NAME = 'SAP_SUSTAINABILITY_PROCESSED';
const LOG_EMAIL = '...';

/***********************
 SAP Build Process Automation
************************/
const SPA_API_URL = "https://spa-api-gateway-bpi-eu-prod.cfapps.eu10.hana.ondemand.com";
const SPA_TOKEN_URL = "https://hackatum.authentication.eu10.hana.ondemand.com/oauth/token";
const SPA_CLIENT_ID = "...";
const SPA_CLIENT_SECRET = "...";
const SPA_API_KEY = "...";
const SPA_ENV = "...";
const SPA_DEFINITION_ID = '...';

/***********************
 SAP AI Core
************************/
const AIC_API_URL = "https://api.ai.prod.eu-central-1.aws.ml.hana.ondemand.com";
const AIC_TOKEN_URL = "https://hackatum.authentication.eu10.hana.ondemand.com/oauth/token";
const AIC_CLIENT_ID = "...";
const AIC_CLIENT_SECRET = "...";
const AIC_RESOURCE_GROUP = "default";
const AIC_DEPLOYMENT_ID = '...';


// Assumes a global constant SHEET_ID is defined

function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      return buildResponse_("Invalid request: no payload received.");
    }

    const payload = JSON.parse(e.postData.contents);
    const invoiceNb = payload.InvoiceNb;
    const successFlag = String(payload.success) === "true";

    if (!invoiceNb) {
      return buildResponse_("Invalid request: no InvoiceNb provided.");
    }

    const ss = SpreadsheetApp.openById(SHEET_ID);
    const sheet = ss.getSheets()[0];

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) {
      // Only header or empty
      return buildResponse_("No data rows available. Invoice not found.");
    }

    const values = sheet.getRange(2, 1, lastRow - 1, 1).getValues(); // first column, starting from row 2
    let rowIndex = null;

    for (let i = 0; i < values.length; i++) {
      if (String(values[i][0]) === String(invoiceNb)) {
        rowIndex = i + 2; // adjust for header row
        break;
      }
    }

    if (rowIndex === null) {
      return buildResponse_("Row not found for the provided InvoiceNb.");
    }

    if (!successFlag) {
      sheet.deleteRow(rowIndex);
      return buildResponse_("Row found and deleted because the success flag was false.");
    } else {
      return buildResponse_("Row found and retained because the success flag was true.");
    }

  } catch (err) {
    return buildResponse_("Error processing request: " + err.message);
  }
}

function buildResponse_(message) {
  const result = {
    message: message,
    timestamp: new Date().toISOString()
  };
  return ContentService
    .createTextOutput(JSON.stringify(result))
    .setMimeType(ContentService.MimeType.JSON);
}

/*******************************
 SHEET STRUCTURE
*******************************/
const SHEET_HEADERS = [
  'InvoiceNb',
  'Date',
  'Supplier',       // name only
  'Material',
  'Energykhw',
  'Litres_Fuel',
  'CloudHours',
  'StorageCloud',
  'DataTransferCloud',
  'TransportMode',
  'DistanceTransport',
  'Amount',
  'Price',
  'Unit',
  'Category',        // new: AI category
  'SupplierEmail'    // new: email address
];


const SUSTAINABILITY_CLASSES = [
  'ENERGY_INVOICE_ELECTRICITY',
  'ENERGY_INVOICE_GAS',
  'WATER_INVOICE',
  'FUEL_INVOICE',
  'WASTE_MANAGEMENT',
  'SERVICE_MAINTENANCE',
  'EMISSIONS_REPORT',
  'GENERAL_CONSUMPTION_INFO',
  'NOT_RELEVANT_FOR_SUSTAINABILITY'
];

// Build a key string for a row object using all sheet headers
function buildRowKeyFromObj_(rowObj) {
  return SHEET_HEADERS
    .map(h => {
      const v = rowObj[h] !== undefined && rowObj[h] !== null ? rowObj[h] : '';
      return String(v).trim();
    })
    .join('||');
}

// Check if row already exists in the sheet (exact match on all columns)
function isDuplicateRow_(sheet, rowObj) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    // Only header row or completely empty
    return false;
  }

  const numCols = SHEET_HEADERS.length;
  const data = sheet.getRange(2, 1, lastRow - 1, numCols).getValues();

  const newKey = buildRowKeyFromObj_(rowObj);

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const existingKey = row
      .map(v => (v !== undefined && v !== null ? String(v).trim() : ''))
      .join('||');

    if (existingKey === newKey) {
      return true;
    }
  }
  return false;
}


/*******************************
 LOG BUFFER
*******************************/
let EXEC_LOG = [];

function log(message) {
  const ts = new Date().toISOString();
  const line = '[' + ts + '] ' + message;
  EXEC_LOG.push(line);
  Logger.log(line);
}

function sendFailureEmail_(error) {
  try {
    const recipient = LOG_EMAIL || Session.getActiveUser().getEmail();
    const subject = 'SAP Sustainability Apps Script failure';
    const body =
      'Error: ' + error + '\n\n' +
      'Stack:\n' + (error && error.stack ? error.stack : 'no stack') + '\n\n' +
      'Execution log:\n\n' + EXEC_LOG.join('\n');

    MailApp.sendEmail(recipient, subject, body);
  } catch (e) {
    Logger.log('Could not send failure email: ' + e);
  }
}

/*******************************
 0. TRIGGER SETUP
*******************************/
function setupTrigger() {
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'processIncomingEmails') {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger('processIncomingEmails')
    .timeBased()
    .everyMinutes(1)
    .create();
}

/*******************************
 1. OAUTH TOKEN
*******************************/
function getOAuthToken(tokenUrl, clientId, clientSecret) {
  const payload = {
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret
  };

  const options = {
    method: 'post',
    payload: payload,
    muteHttpExceptions: true
  };

  let responseText = '';
  try {
    const response = UrlFetchApp.fetch(tokenUrl, options);
    responseText = response.getContentText();
    log('Token response: ' + responseText);
    const json = JSON.parse(responseText);
    if (!json.access_token) {
      log('No access_token in response');
      return null;
    }
    return json.access_token;
  } catch (e) {
    log('Error in getOAuthToken: ' + e + ' raw: ' + responseText);
    return null;
  }
}

/*******************************
 2. MAIN ENTRY (TRIGGER)
*******************************/
function processIncomingEmails() {
  EXEC_LOG = [];
  log('processIncomingEmails start');
  let topError = null;

  try {
    const label = getOrCreateLabel_(PROCESSED_LABEL_NAME);
    const threads = GmailApp.search('in:inbox -label:' + PROCESSED_LABEL_NAME + ' newer_than:7d');
    log('Found threads: ' + threads.length);

    threads.forEach(thread => {
      if (threadHasLabel_(thread, label)) {
        return;
      }
      const messages = thread.getMessages();
      messages.forEach(msg => {
        if (msg.isInInbox() && !msg.isDraft()) {
          log('Handling message with subject: ' + msg.getSubject());
          handleMessage_(msg);
        }
      });
      // label the whole thread once
      label.addToThread(thread);
      log('Label added to thread');
    });

    log('processIncomingEmails finished normally');
  } catch (e) {
    log('Top-level error in processIncomingEmails: ' + e);
    topError = e;
  } finally {
    if (topError) {
      sendFailureEmail_(topError);
      throw topError; // keep Apps Script execution marked as error
    }
  }
}

/*******************************
 3. HANDLE ONE MESSAGE
*******************************/
function handleMessage_(message) {
  const emailData = buildEmailPayload_(message);
  log('Email payload built');

  const category = classifyEmailWithSAPAI_(emailData);
  log('Predicted category: ' + category);

  if (!category || category === 'NOT_RELEVANT_FOR_SUSTAINABILITY') {
    log('Message not relevant for sustainability, skipping sheet and SAP Build');
    return;
  }

  const extracted = extractFieldsWithSAPAI_(category, emailData);
  log('Extracted fields: ' + JSON.stringify(extracted));

  const rowObj = normalizeToSheetHeaders_(extracted, category, emailData);
  log('Normalized row object: ' + JSON.stringify(rowObj));

  const sheet = getOrCreateSheet_();
  ensureHeaders_(sheet);

  if (isDuplicateRow_(sheet, rowObj)) {
    log('Duplicate row detected, skipping append and SAP call');
    return;
  }

  appendRowObject_(sheet, rowObj);
  log('Row appended to sheet: ' + JSON.stringify(rowObj));

  const sapOk = triggerSAPWorkflow_(rowObj);
  if (sapOk) {
    log('SAP workflow started successfully (triggerSAPWorkflow_ returned true)');
  } else {
    log('SAP workflow failed or returned unexpected response (triggerSAPWorkflow_ returned false)');
  }
}

// helper to map normalized object to sheet row in correct column order
function appendRowObject_(sheet, rowObj) {
  const row = SHEET_HEADERS.map(function (h) {
    const v = rowObj && Object.prototype.hasOwnProperty.call(rowObj, h)
      ? rowObj[h]
      : '';
    return v === null || v === undefined ? '' : v;
  });
  sheet.appendRow(row);
}


function appendRowObject_(sheet, rowObj) {
  var rowArray = SHEET_HEADERS.map(function (h) {
    return rowObj && rowObj[h] !== undefined ? rowObj[h] : '';
  });
  sheet.appendRow(rowArray);
}

function testNormalizeAndAppendOnce() {
  var fakeExtracted = {
    InvoiceNb: 'TEST-123',
    Date: '2025-11-23',
    Supplier: 'HackaTUM-X',
    Material: 'Energy',
    Energykhw: 99999999,
    Amount: 99999999,
    Price: 10,
    Unit: 'kWh',
    Category: 'ENERGY_INVOICE_ELECTRICITY',
    SupplierEmail: 'houssem.kotti.deutschland@gmail.com'
  };

  var fakeCategory = 'ENERGY_INVOICE_ELECTRICITY';
  var fakeEmailData = {
    fromName: 'HackaTUM-X',
    fromEmail: 'houssem.kotti.deutschland@gmail.com',
    date: new Date('2025-11-23T00:00:00Z')
  };

  var rowObj = normalizeToSheetHeaders_(fakeExtracted, fakeCategory, fakeEmailData);
  Logger.log('Row object: ' + JSON.stringify(rowObj));

  var ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  var sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  ensureHeaders_(sheet);         
  appendRowObject_(sheet, rowObj);
}




/*******************************
 4. BUILD EMAIL PAYLOAD
*******************************/
function parseEmailAddress_(raw) {
  if (!raw) {
    return { name: '', email: '' };
  }

  // pattern: Name <email@example.com>
  const m = raw.match(/^(.*)<([^>]+)>$/);
  if (m) {
    return {
      name: m[1].trim().replace(/(^"|"$)/g, ''),
      email: m[2].trim()
    };
  }

  // just an email address without name
  return { name: '', email: raw.trim() };
}


function buildEmailPayload_(msg) {
  const fromRaw = msg.getFrom();  // e.g. 'CoolAir Services GmbH <billing@coolair.de>'
  const fromParsed = parseEmailAddress_(fromRaw);

  const payload = {
    subject: msg.getSubject() || '',
    from: fromRaw,
    fromName: fromParsed.name,
    fromEmail: fromParsed.email,
    to: msg.getTo() || '',
    date: msg.getDate(),
    plainBody: msg.getPlainBody() || '',
    htmlBody: msg.getBody() || '',
    attachments: []
  };

  const atts = msg.getAttachments({ includeInlineImages: false, includeAttachments: true }) || [];
  atts.forEach(function (a) {
    payload.attachments.push({
      name: a.getName(),
      contentType: a.getContentType(),
      length: a.getBytes().length
    });
  });

  return payload;
}


/*******************************
 5. SAP AI: CLASSIFICATION
*******************************/
function classifyEmailWithSAPAI_(emailPayload) {
  const token = getOAuthToken(AIC_TOKEN_URL, AIC_CLIENT_ID, AIC_CLIENT_SECRET);
  if (!token) {
    log('No SAP AI token, returning NOT_RELEVANT_FOR_SUSTAINABILITY');
    return 'NOT_RELEVANT_FOR_SUSTAINABILITY';
  }

  const classesList = SUSTAINABILITY_CLASSES.join(', ');
  const prompt =
  'You are a sustainability email classifier. Your ONLY task is to classify the email into one sustainability class.' +
  '\n\nYou MUST assign one of these classes based on KEYWORDS, EVEN IF THERE IS NO ATTACHMENT:' +
  '\n- ENERGY_INVOICE_ELECTRICITY: electricity, kwh, power, energy consumed, meter id, strom, eon, utility bill' +
  '\n- ENERGY_INVOICE_GAS: gas, m³, gasverbrauch, gas supply, heating gas' +
  '\n- WATER_INVOICE: water, wasser, liters, cubic meters, wasserverbrauch, wasserrechnung' +
  '\n- FUEL_INVOICE: diesel, petrol, fuel, liters, fleet, transport fuel, tanken' +
  '\n- WASTE_MANAGEMENT: waste, recycling, disposal, entsorgung' +
  '\n- SERVICE_MAINTENANCE: maintenance, repair, service visit, inspection' +
  '\n- EMISSIONS_REPORT: emissions, co2, greenhouse gas, footprint' +
  '\n- GENERAL_CONSUMPTION_INFO: any utility consumption info that is not a bill' +
  '\n- NOT_RELEVANT_FOR_SUSTAINABILITY: only choose this if ABSOLUTELY nothing relates to consumption, utilities, energy, water, fuel, emissions, waste, or sustainability.' +
  '\n\nEmail JSON:\n' + JSON.stringify(emailPayload) +
  '\n\nReturn ONLY a JSON object like: {"class":"WATER_INVOICE"} with no explanation.';


  const body = {
    orchestration_config: {
      module_configurations: {
        templating_module_config: {
          template: [{ role: 'user', content: prompt }]
        },
        llm_module_config: {
          model_name: 'gemini-2.5-pro',
          model_version: 'latest'
        }
      }
    },
    input_params: {}
  };

  const options = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'ai-resource-group': AIC_RESOURCE_GROUP,
      'content-type': 'application/json'
    },
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(
      AIC_API_URL + '/v2/inference/deployments/' + AIC_DEPLOYMENT_ID + '/completion',
      options
    );
    const text = response.getContentText();
    log('SAP AI classification response: ' + text);
    const parsed = extractJsonFromAIResponse_(text);
    return parsed.class || 'NOT_RELEVANT_FOR_SUSTAINABILITY';
  } catch (e) {
    log('Error in classifyEmailWithSAPAI_: ' + e);
    return 'NOT_RELEVANT_FOR_SUSTAINABILITY';
  }
}

/*******************************
 6. SAP AI: FIELD EXTRACTION
*******************************/
function extractFieldsWithSAPAI_(category, emailPayload) {
  const token = getOAuthToken(AIC_TOKEN_URL, AIC_CLIENT_ID, AIC_CLIENT_SECRET);
  if (!token) {
    log('No SAP AI token, returning empty extraction');
    return {};
  }

  const headersList = SHEET_HEADERS.join(', ');

  const prompt =
    'You are an information extraction assistant for sustainability reporting.\n' +
    'The email is already classified as: ' + category + '.\n\n' +

    'You must fill exactly these fields for ONE invoice line:\n' +
    headersList + '\n\n' +

    'Use the following precise definitions:\n' +
    '- InvoiceNb: invoice number or reference id as string.\n' +
    '- Date: invoice date in ISO format YYYY-MM-DD.\n' +
    '- Supplier: name of the supplier or issuer.\n' +
    '- Material: short description of the billed material or service.\n' +
    '- Energykhw: total electricity consumption in kWh, numeric.\n' +
    '- Litres_Fuel: total fuel quantity in litres, numeric.\n' +
    '- CloudHours: hours of cloud compute, numeric.\n' +
    '- StorageCloud: amount of cloud storage, numeric.\n' +
    '- DataTransferCloud: data transferred in GB, numeric.\n' +
    '- TransportMode: type of transport (for example Truck, Ship, Train, Plane).\n' +
    '- DistanceTransport: distance covered in km, numeric.\n' +
    '- Amount: quantity of the main material or service (for example 7000 for 7000 L of water, or 1842 for 1842 kWh), numeric.\n' +
    '- Price: total monetary cost in EUR for this invoice line. Only a number, no currency symbol, always in EUR.\n' +
    '- Unit: PHYSICAL unit of the Amount, for example L, kWh, m3, kg, t, h, pcs.\n' +
    'IMPORTANT: Unit must NEVER be a currency. Never set Unit to "EUR", "Euro", "USD", "$", "€" or any money symbol.\n' +
    'If the invoice is in another currency (for example USD or GBP), convert to EUR using a reasonable approximate rate and write the converted value in Price.\n' +
    'If a field is unknown, set it to null.\n\n' +

    'Return ONLY a JSON object with exactly these keys and no explanation.\n\n' +
    'Email JSON:\n' + JSON.stringify(emailPayload);

  const body = {
    orchestration_config: {
      module_configurations: {
        templating_module_config: {
          template: [{ role: 'user', content: prompt }]
        },
        llm_module_config: {
          model_name: 'gemini-2.5-pro',
          model_version: 'latest'
        }
      }
    },
    input_params: {}
  };

  const options = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'ai-resource-group': AIC_RESOURCE_GROUP,
      'content-type': 'application/json'
    },
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(
      AIC_API_URL + '/v2/inference/deployments/' + AIC_DEPLOYMENT_ID + '/completion',
      options
    );
    const text = response.getContentText();
    log('SAP AI extraction response: ' + text);
    return extractJsonFromAIResponse_(text);
  } catch (e) {
    log('Error in extractFieldsWithSAPAI_: ' + e);
    return {};
  }
}


/*******************************
 7. PARSE AI RESPONSE
*******************************/
function extractJsonFromAIResponse_(rawText) {
  log('extractJsonFromAIResponse_ raw: ' + rawText);

  try {
    const outer = JSON.parse(rawText);

    // 1) SAP AI Core orchestration_result at top level
    if (
      outer.orchestration_result &&
      outer.orchestration_result.choices &&
      outer.orchestration_result.choices.length > 0
    ) {
      const content = outer.orchestration_result.choices[0].message.content;
      log('Using outer.orchestration_result content: ' + content);
      return parseJsonFromContentString_(content);
    }

    // 2) SAP AI Core llm module inside module_results
    if (
      outer.module_results &&
      outer.module_results.llm &&
      outer.module_results.llm.choices &&
      outer.module_results.llm.choices.length > 0
    ) {
      const content = outer.module_results.llm.choices[0].message.content;
      log('Using module_results.llm content: ' + content);
      return parseJsonFromContentString_(content);
    }

    // 3) Generic OpenAI like structure
    if (outer.choices && outer.choices.length > 0) {
      const content = outer.choices[0].message.content;
      log('Using outer.choices content: ' + content);
      return parseJsonFromContentString_(content);
    }

    // 4) Already flat JSON object
    if (outer.class || outer.InvoiceNb || outer.Date) {
      log('Using outer object directly');
      return outer;
    }

  } catch (e) {
    log('Failed to parse AI response as JSON: ' + e);
  }

  // 5) Fallback: first JSON block in text
  const match = rawText.match(/\{[\s\S]*\}/);
  if (match) {
    try {
      const inner = JSON.parse(match[0]);
      log('Using regex inner JSON: ' + match[0]);
      return inner;
    } catch (e2) {
      log('Failed to parse inner JSON: ' + e2);
    }
  }

  log('Returning empty object from extractJsonFromAIResponse_');
  return {};
}

function parseJsonFromContentString_(content) {
  if (typeof content !== 'string') {
    log('Content is not string in parseJsonFromContentString_');
    return {};
  }

  // Remove ```json ``` fences
  let cleaned = content.replace(/```json/i, '```');
  cleaned = cleaned.replace(/```/g, '').trim();
  log('Cleaned content for JSON parse: ' + cleaned);

  try {
    return JSON.parse(cleaned);
  } catch (e) {
    log('Failed to parse fenced JSON content: ' + e);
    return {};
  }
}


/*******************************
 8. NORMALIZE TO SHEET HEADERS
*******************************/
// Helper to pick first non-null, non-undefined value
function pick_() {
  for (var i = 0; i < arguments.length; i++) {
    var v = arguments[i];
    if (v !== null && v !== undefined && v !== '') {
      return v;
    }
  }
  return '';
}

function normalizeToSheetHeaders_(extracted, category, emailData) {
  extracted = extracted || {};
  emailData = emailData || {};

  var row = {};

  // ['InvoiceNb','Date','Supplier','Material','Energykhw','Litres_Fuel',
  //  'CloudHours','StorageCloud','DataTransferCloud','TransportMode',
  //  'DistanceTransport','Amount','Price','Unit','Category','SupplierEmail']

  // 1) Invoice number
  row.InvoiceNb = pick_(
    extracted.InvoiceNb,
    extracted.invoiceNb,
    extracted.invoice_number,
    extracted.invoiceId,
    extracted.invoice_id
  );

  // 2) Date
  // we allow both ISO string and various names, plus email date fallback
  var dateVal = pick_(
    extracted.Date,
    extracted.invoice_date,
    extracted.date
  );

  if (!dateVal && emailData.date) {
    var d = emailData.date instanceof Date ? emailData.date : new Date(emailData.date);
    if (!isNaN(d.getTime())) {
      dateVal = Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
    }
  }
  row.Date = dateVal || '';

  // 3) Supplier name
  row.Supplier = pick_(
    extracted.Supplier,
    extracted.supplier_name,
    emailData.fromName
  );

  // 4) Material / description
  row.Material = pick_(
    extracted.Material,
    extracted.material,
    extracted.description,
    extracted.line_item_description
  );

  // 5) Energy kWh
  row.Energykhw = pick_(
    extracted.Energykhw,
    extracted.energy_kwh,
    extracted.quantity_kwh,
    extracted.energyKwh
  );

  // 6) Fuel litres
  row.Litres_Fuel = pick_(
    extracted.Litres_Fuel,
    extracted.litres_fuel,
    extracted.fuel_litres
  );

  // 7) CloudHours
  row.CloudHours = pick_(
    extracted.CloudHours,
    extracted.cloud_hours,
    extracted.compute_hours
  );

  // 8) StorageCloud
  row.StorageCloud = pick_(
    extracted.StorageCloud,
    extracted.storage_gb_month,
    extracted.storage_usage_gb_month
  );

  // 9) DataTransferCloud
  row.DataTransferCloud = pick_(
    extracted.DataTransferCloud,
    extracted.transfer_gb,
    extracted.data_transfer_gb
  );

  // 10) TransportMode
  row.TransportMode = pick_(
    extracted.TransportMode,
    extracted.transport_mode
  );

  // 11) DistanceTransport
  row.DistanceTransport = pick_(
    extracted.DistanceTransport,
    extracted.distance_km
  );

  // 12) Amount
  row.Amount = pick_(
    extracted.Amount,
    extracted.amount,
    extracted.quantity
  );

  // 13) Price (EUR)
  row.Price = pick_(
    extracted.Price,
    extracted.total_amount,
    extracted.price_eur,
    extracted.price
  );

  // 14) Unit (material unit, not currency)
  row.Unit = pick_(
    extracted.Unit,
    extracted.unit,
    extracted.uom
  );

  // 15) Category
  row.Category = pick_(
    extracted.Category,
    category
  );

  // 16) SupplierEmail
  row.SupplierEmail = pick_(
    extracted.SupplierEmail,
    emailData.fromEmail
  );

  return row;
}




/*******************************
 9. SHEET HELPERS
*******************************/
function getOrCreateSheet_() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const sheets = ss.getSheets();
  return sheets.length > 0 ? sheets[0] : ss.insertSheet('main');
}

function ensureHeaders_(sheet) {
  const firstRow = sheet.getRange(1, 1, 1, SHEET_HEADERS.length).getValues()[0];
  const isEmpty = firstRow.every(v => v === '');
  if (isEmpty) {
    sheet.getRange(1, 1, 1, SHEET_HEADERS.length).setValues([SHEET_HEADERS]);
  }
}

/*******************************
 10. SAP BUILD WORKFLOW
*******************************/
function triggerSAPWorkflow_(contextObj) {
  const token = getOAuthToken(SPA_TOKEN_URL, SPA_CLIENT_ID, SPA_CLIENT_SECRET);
  if (!token) {
    log('No SAP Build token – returning false');
    return false;
  }

  const url = `${SPA_API_URL}/workflow/rest/v1/workflow-instances?environmentId=${SPA_ENV}`;

  const body = {
    definitionId: SPA_DEFINITION_ID,
    context: contextObj
  };

  const options = {
    method: 'post',
    headers: {
      Authorization: 'Bearer ' + token,
      'api-key': SPA_API_KEY,
      'content-type': 'application/json'
    },
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  };

  try {
    const response = UrlFetchApp.fetch(url, options); // waits fully
    const text = response.getContentText();
    log('SAP Build raw response: ' + text);

    // If SAP Build returns an error, it always contains an "error" object
    let json;
    try {
      json = JSON.parse(text);
    } catch (e) {
      log('Could not parse SAP response JSON');
      return false;
    }

    // A valid started workflow always contains: id + startedAt
    if (json && json.id && json.startedAt) {
      log('SAP Workflow started successfully (TRUE)');
      return true;
    }

    // Otherwise treat as FALSE
    log('SAP returned unexpected or error structure (FALSE)');
    return false;

  } catch (e) {
    log('Exception during SAP Build call: ' + e);
    return false;
  }
}


/*******************************
 11. GMAIL LABEL HELPERS
*******************************/
function getOrCreateLabel_(name) {
  return GmailApp.getUserLabelByName(name) || GmailApp.createLabel(name);
}

function threadHasLabel_(thread, label) {
  return thread.getLabels().some(l => l.getName() === label.getName());
}


/*********************************
 * CONFIG
 *********************************/
const DASHBOARD_SHEET_ID = SHEET_ID;  
const DASHBOARD_SHEET_NAME = 'Emails';            

// Column mapping (0 based)
const COL_INVOICE_NB       = 0;
const COL_DATE             = 1;
const COL_SUPPLIER         = 2;
const COL_MATERIAL         = 3;
const COL_ENERGY_KWH       = 4;
const COL_FUEL_LITRES      = 5;
const COL_CLOUD_HOURS      = 6;
const COL_STORAGE_GBMONTH  = 7;
const COL_TRANSFER_GB      = 8;
const COL_TRANSPORT_MODE   = 9;
const COL_DISTANCE_KM      = 10;
const COL_AMOUNT           = 11;
const COL_PRICE_EUR        = 12;
const COL_UNIT             = 13;
const COL_CATEGORY         = 14;
const COL_SUPPLIER_EMAIL   = 15; 



/*********************************
 * FRONTEND ENTRY POINT
 *********************************/
function doGet(e) {
  return HtmlService
    .createTemplateFromFile('index')
    .evaluate()
    .setTitle('Sustainability Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}


/*********************************
 * API: DASHBOARD SUMMARY
 *********************************/
function getDashboardSummary(startDateIso, endDateIso) {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return {
      totalInvoices: 0,
      totalPriceEur: 0,
      totalEnergyKwh: 0,
      totalFuelLitres: 0,
      totalCloudHours: 0,
      totalStorageGbMonth: 0,
      totalTransferGb: 0
    };
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  // parse filter dates (inclusive)
  let startDate = null;
  let endDate = null;
  if (startDateIso && String(startDateIso).trim() !== "") {
    startDate = new Date(startDateIso);
  }
  if (endDateIso && String(endDateIso).trim() !== "") {
    endDate = new Date(endDateIso);
  }

  const seenKeys = new Set();

  let totalInvoices       = 0;
  let totalPriceEur       = 0;
  let totalEnergyKwh      = 0;
  let totalFuelLitres     = 0;
  let totalCloudHours     = 0;
  let totalStorageGbMonth = 0;
  let totalTransferGb     = 0;

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    // sheet Date column
    let rowDate = row[COL_DATE];

    // skip empty date when filters are set
    if ((startDate || endDate) && !rowDate) {
      continue;
    }

    if (rowDate) {
      if (!(rowDate instanceof Date)) {
        rowDate = new Date(rowDate);
      }
      if (startDate && rowDate < startDate) {
        continue;
      }
      if (endDate && rowDate > endDate) {
        continue;
      }
    }

    const isAllEmpty =
      !row[COL_DATE] &&
      !row[COL_ENERGY_KWH] &&
      !row[COL_FUEL_LITRES] &&
      !row[COL_CLOUD_HOURS] &&
      !row[COL_STORAGE_GBMONTH] &&
      !row[COL_TRANSFER_GB] &&
      !row[COL_PRICE_EUR];

    if (isAllEmpty) {
      continue;
    }

    const key = buildRowKey_(row);
    if (seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);

    totalInvoices++;

    totalPriceEur       += toNumber_(row[COL_PRICE_EUR]);
    totalEnergyKwh      += toNumber_(row[COL_ENERGY_KWH]);
    totalFuelLitres     += toNumber_(row[COL_FUEL_LITRES]);
    totalCloudHours     += toNumber_(row[COL_CLOUD_HOURS]);
    totalStorageGbMonth += toNumber_(row[COL_STORAGE_GBMONTH]);
    totalTransferGb     += toNumber_(row[COL_TRANSFER_GB]);
  }

  return {
    totalInvoices,
    totalPriceEur,
    totalEnergyKwh,
    totalFuelLitres,
    totalCloudHours,
    totalStorageGbMonth,
    totalTransferGb
  };
}


/*********************************
 * HELPERS
 *********************************/

// build a key string for duplicate detection
function buildRowKey_(row) {
  return row
    .map(v => (v === null || v === undefined ? '' : String(v).trim()))
    .join('||');
}

function toNumber_(value) {
  if (value === null || value === undefined || value === '') {
    return 0;
  }
  const s = String(value).replace(',', '.');
  const num = parseFloat(s);
  return isNaN(num) ? 0 : num;
}

function getAvailableYears() {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return [];
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const yearsSet = {};

  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    let d = row[COL_DATE];
    if (!d) continue;

    if (!(d instanceof Date)) {
      d = new Date(d);
      if (isNaN(d.getTime())) continue;
    }

    const year = d.getFullYear();
    if (!yearsSet[year]) {
      yearsSet[year] = true;
    }
  }

  const years = Object.keys(yearsSet)
    .map(function (y) { return parseInt(y, 10); })
    .sort(function (a, b) { return a - b; });

  return years;
}

function buildHistogramNarrativeWithSAPAI_(year, invoiceAmounts) {
  if (!invoiceAmounts || invoiceAmounts.length === 0) {
    return 'No invoice amounts were available for this period.';
  }

  const stats = computeSimpleStats_(invoiceAmounts);

  const prompt =
    'You are an ESG reporting assistant. You receive summary statistics of invoice amounts for the year ' +
    year +
    ' in this JSON: ' +
    JSON.stringify(stats) +
    '. ' +
    'Write 1 to 2 short paragraphs that describe the distribution of invoice sizes, highlighting whether the cost structure is dominated by many small invoices or a few large ones. ' +
    'Use plain text only, no markdown or code.';

  const text = callSapAiForNarrative_(prompt);
  if (text) return text;

  return (
    'The distribution of invoice amounts indicates that most invoices fall in the lower value range, with a few large documents driving a significant share of the annual spend. ' +
    'This pattern is typical for utilities, services and cloud charges consolidated into periodic statements.'
  );
}


function generateEsgReportForYear(yearStr) {
  if (!yearStr) {
    log('generateEsgReportForYear called without year');
    return null;
  }

  const year = parseInt(yearStr, 10);
  if (isNaN(year)) {
    log('generateEsgReportForYear: invalid year ' + yearStr);
    return null;
  }

  const startIso = year + '-01-01';
  const endIso   = year + '-12-31';

  const summary = getDashboardSummary(startIso, endIso);
  if (!summary) {
    log('generateEsgReportForYear: no summary for year ' + year);
    return null;
  }

  const monthlySeries   = getMonthlyTimeSeries(startIso, endIso) || [];
  const spendByCategory = getSpendByCategory(startIso, endIso) || [];
  const invoiceAmounts  = getInvoiceAmounts(startIso, endIso) || [];

  const overallNarrative    = buildEsgNarrativeWithSAPAI_(year, summary, monthlySeries, spendByCategory);
  const timeSeriesNarrative = buildTimeSeriesNarrativeWithSAPAI_(year, monthlySeries);
  const categoryNarrative   = buildCategoryNarrativeWithSAPAI_(year, spendByCategory);
  const histogramNarrative  = buildHistogramNarrativeWithSAPAI_(year, invoiceAmounts);

  const file = createEsgPdfFile_(year, {
    summary,
    overallNarrative,
    timeSeriesNarrative,
    categoryNarrative,
    histogramNarrative,
    monthlySeries,
    spendByCategory,
    invoiceAmounts
  });

  return file ? file.getId() : null;
}

function callSapAiForNarrative_(prompt) {
  try {
    const token = getOAuthToken(AIC_TOKEN_URL, AIC_CLIENT_ID, AIC_CLIENT_SECRET);
    if (!token) {
      log('callSapAiForNarrative_: no token');
      return null;
    }

    const body = {
      orchestration_config: {
        module_configurations: {
          templating_module_config: {
            template: [{ role: 'user', content: prompt }]
          },
          llm_module_config: {
            model_name: 'gemini-2.5-pro',
            model_version: 'latest'
          }
        }
      },
      input_params: {}
    };

    const options = {
      method: 'post',
      headers: {
        Authorization: 'Bearer ' + token,
        'ai-resource-group': AIC_RESOURCE_GROUP,
        'content-type': 'application/json'
      },
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    };

    const url = AIC_API_URL + '/v2/inference/deployments/' + AIC_DEPLOYMENT_ID + '/completion';
    const response = UrlFetchApp.fetch(url, options);
    const text = response.getContentText();
    log('callSapAiForNarrative_ raw: ' + text);

    const outer = JSON.parse(text);
    let content = '';

    if (outer.orchestration_result &&
        outer.orchestration_result.choices &&
        outer.orchestration_result.choices.length > 0 &&
        outer.orchestration_result.choices[0].message &&
        outer.orchestration_result.choices[0].message.content) {
      content = outer.orchestration_result.choices[0].message.content;
    } else if (outer.module_results &&
               outer.module_results.llm &&
               outer.module_results.llm.choices &&
               outer.module_results.llm.choices.length > 0 &&
               outer.module_results.llm.choices[0].message &&
               outer.module_results.llm.choices[0].message.content) {
      content = outer.module_results.llm.choices[0].message.content;
    } else {
      content = text;
    }

    if (typeof content === 'string') {
      let cleaned = content.trim();
      cleaned = cleaned.replace(/```[a-zA-Z]*\n?/g, '').replace(/```/g, '').trim();
      cleaned = cleaned.replace(/^#{1,6}\s*/g, '').trim();
      return cleaned;
    }

    return null;
  } catch (e) {
    log('callSapAiForNarrative_ error: ' + e);
    return null;
  }
}


function aggregateSummaryForYear_(year) {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return {
      totalInvoices: 0,
      totalPriceEur: 0,
      totalEnergyKwh: 0,
      totalFuelLitres: 0,
      totalCloudHours: 0,
      totalStorageGbMonth: 0,
      totalTransferGb: 0
    };
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const seenKeys = new Set();

  let totalInvoices       = 0;
  let totalPriceEur       = 0;
  let totalEnergyKwh      = 0;
  let totalFuelLitres     = 0;
  let totalCloudHours     = 0;
  let totalStorageGbMonth = 0;
  let totalTransferGb     = 0;

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    // 1) robust date handling
    let d = row[COL_DATE];
    if (!d) {
      continue;
    }
    if (!(d instanceof Date)) {
      d = new Date(d);
      if (isNaN(d.getTime())) {
        continue;
      }
    }
    const rowYear = d.getFullYear();
    if (rowYear !== year) {
      continue;
    }

    // 2) skip rows that are "empty" on key numeric fields
    const isAllEmpty =
      !row[COL_DATE] &&
      !row[COL_ENERGY_KWH] &&
      !row[COL_FUEL_LITRES] &&
      !row[COL_CLOUD_HOURS] &&
      !row[COL_STORAGE_GBMONTH] &&
      !row[COL_TRANSFER_GB] &&
      !row[COL_PRICE_EUR];

    if (isAllEmpty) {
      continue;
    }

    // 3) deduplicate on full-row key
    const key = buildRowKey_(row);
    if (seenKeys.has(key)) {
      continue;
    }
    seenKeys.add(key);

    // 4) aggregate
    totalInvoices++;
    totalPriceEur       += toNumber_(row[COL_PRICE_EUR]);
    totalEnergyKwh      += toNumber_(row[COL_ENERGY_KWH]);
    totalFuelLitres     += toNumber_(row[COL_FUEL_LITRES]);
    totalCloudHours     += toNumber_(row[COL_CLOUD_HOURS]);
    totalStorageGbMonth += toNumber_(row[COL_STORAGE_GBMONTH]);
    totalTransferGb     += toNumber_(row[COL_TRANSFER_GB]);
  }

  return {
    totalInvoices,
    totalPriceEur,
    totalEnergyKwh,
    totalFuelLitres,
    totalCloudHours,
    totalStorageGbMonth,
    totalTransferGb
  };
}

function buildTimeSeriesNarrativeWithSAPAI_(year, monthlySeries) {
  if (!monthlySeries || monthlySeries.length === 0) {
    return 'No monthly data was available for the selected period.';
  }

  const trimmed = monthlySeries.map(function (m) {
    return {
      month: m.month,
      energyKwh: m.totalEnergyKwh,
      spendEur: m.totalPriceEur
    };
  });

  const prompt =
    'You are an ESG analyst. You receive monthly data for one year in JSON with the fields "month", "energyKwh" and "spendEur". ' +
    'Explain in 2 to 3 short paragraphs how energy consumption and related spend evolved during the year ' +
    year +
    '. ' +
    'Comment on peaks, troughs and any noticeable correlation between kWh and spend. ' +
    'Answer with plain text, no bullets or markdown.\n\n' +
    JSON.stringify(trimmed);

  const text = callSapAiForNarrative_(prompt);
  if (text) return text;

  return 'The monthly time series shows how electricity consumption and related spend developed throughout the year, with clear seasonal fluctuations and a visible link between kWh and cost.';
}

function buildCategoryNarrativeWithSAPAI_(year, spendByCategory) {
  if (!spendByCategory || spendByCategory.length === 0) {
    return 'There was no classified spend by sustainability category in this period.';
  }

  const prompt =
    'You are an ESG analyst. You receive a JSON array with objects { "category": "...", "totalPriceEur": number } representing the annual spend by sustainability category for the year ' +
    year +
    '. ' +
    'Summarize in 2 short paragraphs which categories dominate the spend and what that implies for the environmental footprint. ' +
    'Answer with plain text, no bullets or markdown.\n\n' +
    JSON.stringify(spendByCategory);

  const text = callSapAiForNarrative_(prompt);
  if (text) return text;

  return 'The spend by category chart highlights which sustainability related cost centers dominate the year, such as energy, fuel, water, waste and emissions reporting.';
}

function buildInvoiceHistogramImage_(invoiceAmounts) {
  if (!invoiceAmounts || invoiceAmounts.length === 0) return null;

  const values = invoiceAmounts.slice().sort(function (a, b) { return a - b; });
  const min = values[0];
  const max = values[values.length - 1];

  if (min === max) {
    const dataTableSingle = Charts.newDataTable()
      .addColumn(Charts.ColumnType.STRING, 'Range')
      .addColumn(Charts.ColumnType.NUMBER, 'Count')
      .addRow([String(Math.round(min)), values.length]);

    const singleChart = Charts.newColumnChart()
      .setDataTable(dataTableSingle)
      .setDimensions(900, 380)
      .setOption('legend', 'none')
      .setOption('colors', ['#008fd3'])
      .setOption('hAxis', { title: 'Invoice amount (EUR)' })
      .setOption('vAxis', { title: 'Count' })
      .build();

    return singleChart.getAs('image/png');
  }

  var bucketCount = 10;
  var range = max - min;
  if (range < bucketCount) {
    bucketCount = Math.max(3, Math.floor(range) || 3);
  }
  const bucketSize = range / bucketCount;

  const buckets = [];
  for (var i = 0; i < bucketCount; i++) {
    const start = min + i * bucketSize;
    const end = start + bucketSize;
    buckets.push({
      label: Math.round(start) + ' - ' + Math.round(end),
      count: 0
    });
  }

  values.forEach(function (v) {
    var index = Math.floor((v - min) / bucketSize);
    if (index >= bucketCount) index = bucketCount - 1;
    if (index < 0) index = 0;
    buckets[index].count++;
  });

  const dataTable = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, 'Range')
    .addColumn(Charts.ColumnType.NUMBER, 'Count');

  buckets.forEach(function (b) {
    dataTable.addRow([b.label, b.count]);
  });

  const chart = Charts.newColumnChart()
    .setDataTable(dataTable)
    .setDimensions(900, 380)
    .setOption('legend', 'none')
    .setOption('colors', ['#008fd3'])
    .setOption('hAxis', {
      title: 'Invoice amount (EUR)',
      slantedText: true,
      slantedTextAngle: 45
    })
    .setOption('vAxis', { title: 'Count' })
    .build();

  return chart.getAs('image/png');
}



function computeSimpleStats_(arr) {
  if (!arr || arr.length === 0) {
    return { count: 0, min: 0, max: 0, mean: 0 };
  }
  const sorted = arr.slice().sort(function (a, b) {
    return a - b;
  });
  const count = sorted.length;
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  let sum = 0;
  for (var i = 0; i < sorted.length; i++) {
    sum += sorted[i];
  }
  const mean = sum / count;
  const p50 = sorted[Math.floor(0.5 * (count - 1))];
  const p90 = sorted[Math.floor(0.9 * (count - 1))];

  return {
    count: count,
    min: min,
    max: max,
    mean: mean,
    median: p50,
    p90: p90
  };
}



function buildEsgNarrativeWithSAPAI_(year, summary, monthlySeries, spendByCategory) {
  const baseStats = {
    totalInvoices: summary.totalInvoices,
    totalPriceEur: summary.totalPriceEur,
    totalEnergyKwh: summary.totalEnergyKwh,
    totalFuelLitres: summary.totalFuelLitres,
    totalCloudHours: summary.totalCloudHours,
    totalStorageGbMonth: summary.totalStorageGbMonth,
    totalTransferGb: summary.totalTransferGb
  };

  const context = {
    year,
    totals: baseStats,
    monthlyPoints: monthlySeries.length,
    categories: spendByCategory.map(c => ({
      category: c.category,
      spendEur: c.totalPriceEur
    }))
  };

  const prompt =
    'You are helping to write a professional ESG sustainability report for the company SAP EcoTrace for the year ' +
    year +
    '. ' +
    'Use the following JSON with aggregated KPIs and category spend to write a compact executive summary (4 to 6 paragraphs):\n\n' +
    JSON.stringify(context) +
    '\n\n' +
    'Focus on energy and fuel consumption, cloud and digital infrastructure, spend structure by sustainability category, and risks or opportunities. ' +
    'Write in a neutral, business style that could be used directly in a board level ESG report. ' +
    'Do not include any markdown, bullet points, titles or code fences, only plain text paragraphs.';

  const text = callSapAiForNarrative_(prompt);

  if (text) {
    return text;
  }

  // Fallback
  return (
    'In the reporting year ' +
    year +
    ' the company processed ' +
    baseStats.totalInvoices +
    ' sustainability relevant invoices. Total spend covered by the system amounted to approximately ' +
    baseStats.totalPriceEur.toFixed(2) +
    ' EUR. The main cost drivers were energy, fuel and cloud infrastructure. ' +
    'The data set provides a robust foundation for tracking environmental performance and preparing the ESG disclosures.'
  );
}



function createEsgPdfFile_(year, data) {
  const summary             = data.summary;
  const overallNarrative    = data.overallNarrative;
  const timeSeriesNarrative = data.timeSeriesNarrative;
  const categoryNarrative   = data.categoryNarrative;
  const histogramNarrative  = data.histogramNarrative;
  const monthlySeries       = data.monthlySeries || [];
  const spendByCategory     = data.spendByCategory || [];
  const invoiceAmounts      = data.invoiceAmounts || [];

  const logoUrl = 'https://lh3.googleusercontent.com/d/1iYWqyPQV9elWueU675VGjRXKkNdJEnkB';

  const docName = 'ESG_Sustainability_Report_' + year;
  const doc = DocumentApp.create(docName);
  const body = doc.getBody();
  body.clear();

  // Margins
  body.setMarginTop(36);
  body.setMarginBottom(36);
  body.setMarginLeft(50);
  body.setMarginRight(36);

  // Logo (force banner-like aspect)
  try {
    const logoResp = UrlFetchApp.fetch(logoUrl);
    const logoBlob = logoResp.getBlob().setName('ecotrace-logo.png');

    const pLogo = body.appendParagraph('');
    const img = pLogo.appendInlineImage(logoBlob);

    // Force a wide banner shape (adjust to taste)
    img.setWidth(200);
    img.setHeight(60);

    pLogo.setAlignment(DocumentApp.HorizontalAlignment.LEFT);
  } catch (e) {
    log('Could not fetch logo: ' + e);
  }


  // Title and metadata
  body.appendParagraph('SAP EcoTrace ESG Sustainability Report ' + year)
      .setHeading(DocumentApp.ParagraphHeading.TITLE);

  const createdStr = Utilities.formatDate(
    new Date(),
    Session.getScriptTimeZone(),
    'yyyy-MM-dd HH:mm'
  );
  body.appendParagraph('Created on: ' + createdStr)
      .setForegroundColor('#555555')
      .setSpacingAfter(16);

  body.appendParagraph('1. Executive summary')
      .setHeading(DocumentApp.ParagraphHeading.HEADING1);

  body.appendParagraph(overallNarrative).setSpacingAfter(14);

  // Key metrics section
  body.appendParagraph('2. Key environmental metrics')
      .setHeading(DocumentApp.ParagraphHeading.HEADING1);

  const metricsTable = body.appendTable([
    ['Metric', 'Value', 'Unit'],
    ['Invoices processed', summary.totalInvoices, ''],
    ['Total spend', summary.totalPriceEur.toFixed(2), 'EUR'],
    ['Electricity usage', summary.totalEnergyKwh.toFixed(3), 'kWh'],
    ['Fuel consumption', summary.totalFuelLitres.toFixed(3), 'L'],
    ['Cloud compute', summary.totalCloudHours.toFixed(3), 'CloudHours'],
    ['Cloud storage', summary.totalStorageGbMonth.toFixed(3), 'GB-month'],
    ['Data transfer', summary.totalTransferGb.toFixed(3), 'GB']
  ]);
  metricsTable.getRow(0).editAsText().setBold(true);
  metricsTable.setBorderWidth(0.5);
  body.appendParagraph('').setSpacingAfter(4);

  // Charts section
  body.appendParagraph('3. Visual analysis')
      .setHeading(DocumentApp.ParagraphHeading.HEADING1);

  // 3.1 Time series chart
  body.appendParagraph('3.1 Monthly energy consumption and spend')
      .setHeading(DocumentApp.ParagraphHeading.HEADING2);

  if (monthlySeries.length > 0) {
    const tsChart = buildTimeSeriesChartImage_(monthlySeries);
    if (tsChart) {
      const pChart = body.appendParagraph('');
      pChart.appendInlineImage(tsChart).setWidth(460);
      pChart.setAlignment(DocumentApp.HorizontalAlignment.CENTER);
    }
    body.appendParagraph(timeSeriesNarrative).setSpacingAfter(14);
  } else {
    body.appendParagraph('No monthly data is available for this period.').setSpacingAfter(14);
  }

  // 3.2 Spend by category
  body.appendParagraph('3.2 Spend by sustainability category')
      .setHeading(DocumentApp.ParagraphHeading.HEADING2);

  if (spendByCategory.length > 0) {
    const pieBlob = buildCategoryPieChartImage_(spendByCategory);
    if (pieBlob) {
      const pChart2 = body.appendParagraph('');
      pChart2.appendInlineImage(pieBlob).setWidth(360);
      pChart2.setAlignment(DocumentApp.HorizontalAlignment.CENTER);
    }
    body.appendParagraph(categoryNarrative).setSpacingAfter(14);
  } else {
    body.appendParagraph('No classified spend by category is available for this period.').setSpacingAfter(14);
  }

  // 3.3 Invoice amount distribution
  body.appendParagraph('3.3 Distribution of invoice amounts')
      .setHeading(DocumentApp.ParagraphHeading.HEADING2);

  if (invoiceAmounts.length > 0) {
    const histBlob = buildInvoiceHistogramImage_(invoiceAmounts);
    if (histBlob) {
      const pChart3 = body.appendParagraph('');
      pChart3.appendInlineImage(histBlob).setWidth(360);
      pChart3.setAlignment(DocumentApp.HorizontalAlignment.CENTER);
    }
    body.appendParagraph(histogramNarrative).setSpacingAfter(14);
  } else {
    body.appendParagraph('No invoice amount information is available for this period.').setSpacingAfter(14);
  }

  // Closing section
  body.appendParagraph('4. Outlook and next steps')
      .setHeading(DocumentApp.ParagraphHeading.HEADING1);

  body.appendParagraph(
    'The current dataset provides a solid basis for tracking environmental performance over time. ' +
    'In the next expansion step, SAP EcoTrace can be extended with emission factors for electricity, fuel, cloud usage and logistics in order to calculate Scope 1, Scope 2 and selected Scope 3 greenhouse gas emissions. ' +
    'On top of that, the same pipeline can be reused to support CSRD reporting templates and internal management cockpits.'
  );

  doc.saveAndClose();

  const pdfBlob = doc.getAs('application/pdf').setName(docName + '.pdf');
  const file = DriveApp.createFile(pdfBlob);

  return file;
}

function buildTimeSeriesChartImage_(monthlySeries) {
  if (!monthlySeries || monthlySeries.length === 0) return null;

  const dataTable = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, 'Month')
    .addColumn(Charts.ColumnType.NUMBER, 'Energy kWh')
    .addColumn(Charts.ColumnType.NUMBER, 'Spend EUR');

  monthlySeries.forEach(function (m) {
    dataTable.addRow([
      String(m.month),
      Number(m.totalEnergyKwh || 0),
      Number(m.totalPriceEur || 0)
    ]);
  });

  const chart = Charts.newLineChart()
    .setDataTable(dataTable)
    // high resolution; we will scale it down in the Doc
    .setDimensions(1200, 380)
    .setOption('legend', { position: 'bottom' })
    .setOption('colors', ['#2ea64a', '#008fd3'])
    .setOption('hAxis', { slantedText: true, slantedTextAngle: 45 })
    .setOption('vAxis', { title: 'kWh / EUR' })
    .build();

  return chart.getAs('image/png');
}


function buildCategoryPieChartImage_(spendByCategory) {
  if (!spendByCategory || spendByCategory.length === 0) return null;

  const dataTable = Charts.newDataTable()
    .addColumn(Charts.ColumnType.STRING, 'Category')
    .addColumn(Charts.ColumnType.NUMBER, 'Spend EUR');

  spendByCategory.forEach(function (c) {
    dataTable.addRow([String(c.category), Number(c.totalPriceEur || 0)]);
  });

  const chart = Charts.newPieChart()
    .setDataTable(dataTable)
    .setDimensions(900, 380)
    .setOption('pieHole', 0.45)
    .setOption('legend', { position: 'right', textStyle: { fontSize: 10 } })
    .setOption('chartArea', { left: 10, top: 10, width: '75%', height: '85%' })
    .setOption('colors', [
      '#2ea64a', '#008fd3', '#7bcf6a', '#4b8ac9',
      '#f4b400', '#e67c73', '#a142f4', '#0f9d58'
    ])
    .build();

  return chart.getAs('image/png');
}


/*********************************
 * INCLUDE HTML PARTIALS IF NEEDED
 *********************************/
function include(filename) {
  return HtmlService.createHtmlOutputFromFile(filename).getContent();
}

function getMonthlyTimeSeries(startDateIso, endDateIso) {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return [];
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const tz = Session.getScriptTimeZone();

  let startDate = null;
  let endDate = null;
  if (startDateIso && String(startDateIso).trim() !== '') {
    startDate = new Date(startDateIso);
  }
  if (endDateIso && String(endDateIso).trim() !== '') {
    endDate = new Date(endDateIso);
  }

  const seenKeys = new Set();
  const monthly = {};

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    let rowDate = row[COL_DATE];
    if (rowDate) {
      if (!(rowDate instanceof Date)) {
        rowDate = new Date(rowDate);
      }
      if (isNaN(rowDate.getTime())) continue;
      if (startDate && rowDate < startDate) continue;
      if (endDate && rowDate > endDate) continue;
    } else if (startDate || endDate) {
      continue;
    }

    const isAllEmpty =
      !row[COL_DATE] &&
      !row[COL_ENERGY_KWH] &&
      !row[COL_FUEL_LITRES] &&
      !row[COL_CLOUD_HOURS] &&
      !row[COL_STORAGE_GBMONTH] &&
      !row[COL_TRANSFER_GB] &&
      !row[COL_PRICE_EUR];

    if (isAllEmpty) continue;

    const key = buildRowKey_(row);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    let monthKey = 'Unknown';
    if (rowDate) {
      monthKey = Utilities.formatDate(rowDate, tz, 'yyyy-MM');
    }

    if (!monthly[monthKey]) {
      monthly[monthKey] = {
        month: monthKey,
        totalPriceEur: 0,
        totalEnergyKwh: 0,
        totalFuelLitres: 0,
        totalCloudHours: 0,
        totalStorageGbMonth: 0,
        totalTransferGb: 0
      };
    }

    const m = monthly[monthKey];
    m.totalPriceEur       += toNumber_(row[COL_PRICE_EUR]);
    m.totalEnergyKwh      += toNumber_(row[COL_ENERGY_KWH]);
    m.totalFuelLitres     += toNumber_(row[COL_FUEL_LITRES]);
    m.totalCloudHours     += toNumber_(row[COL_CLOUD_HOURS]);
    m.totalStorageGbMonth += toNumber_(row[COL_STORAGE_GBMONTH]);
    m.totalTransferGb     += toNumber_(row[COL_TRANSFER_GB]);
  }

  const keys = Object.keys(monthly).sort();
  return keys.map(k => monthly[k]);
}

function getSpendByCategory(startDateIso, endDateIso) {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return [];
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  let startDate = null;
  let endDate = null;
  if (startDateIso && String(startDateIso).trim() !== '') {
    startDate = new Date(startDateIso);
  }
  if (endDateIso && String(endDateIso).trim() !== '') {
    endDate = new Date(endDateIso);
  }

  const seenKeys = new Set();
  const byCategory = {};

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    let rowDate = row[COL_DATE];
    if (rowDate) {
      if (!(rowDate instanceof Date)) {
        rowDate = new Date(rowDate);
      }
      if (isNaN(rowDate.getTime())) continue;
      if (startDate && rowDate < startDate) continue;
      if (endDate && rowDate > endDate) continue;
    } else if (startDate || endDate) {
      continue;
    }

    const isAllEmpty =
      !row[COL_DATE] &&
      !row[COL_ENERGY_KWH] &&
      !row[COL_FUEL_LITRES] &&
      !row[COL_CLOUD_HOURS] &&
      !row[COL_STORAGE_GBMONTH] &&
      !row[COL_TRANSFER_GB] &&
      !row[COL_PRICE_EUR];

    if (isAllEmpty) continue;

    const key = buildRowKey_(row);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    let cat = row[COL_CATEGORY];
    if (!cat || String(cat).trim() === '') {
      cat = 'Uncategorized';
    } else {
      cat = String(cat).trim();
    }

    const price = toNumber_(row[COL_PRICE_EUR]);
    if (!byCategory[cat]) byCategory[cat] = 0;
    byCategory[cat] += price;
  }

  const cats = Object.keys(byCategory).sort();
  return cats.map(c => ({ category: c, totalPriceEur: byCategory[c] }));
}

function getInvoiceAmounts(startDateIso, endDateIso) {
  const ss = SpreadsheetApp.openById(DASHBOARD_SHEET_ID);
  const sheet = ss.getSheetByName(DASHBOARD_SHEET_NAME) || ss.getSheets()[0];

  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 2) {
    return [];
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();

  let startDate = null;
  let endDate = null;
  if (startDateIso && String(startDateIso).trim() !== '') {
    startDate = new Date(startDateIso);
  }
  if (endDateIso && String(endDateIso).trim() !== '') {
    endDate = new Date(endDateIso);
  }

  const seenKeys = new Set();
  const amounts = [];

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    let rowDate = row[COL_DATE];
    if (rowDate) {
      if (!(rowDate instanceof Date)) {
        rowDate = new Date(rowDate);
      }
      if (isNaN(rowDate.getTime())) continue;
      if (startDate && rowDate < startDate) continue;
      if (endDate && rowDate > endDate) continue;
    } else if (startDate || endDate) {
      continue;
    }

    const isAllEmpty =
      !row[COL_DATE] &&
      !row[COL_ENERGY_KWH] &&
      !row[COL_FUEL_LITRES] &&
      !row[COL_CLOUD_HOURS] &&
      !row[COL_STORAGE_GBMONTH] &&
      !row[COL_TRANSFER_GB] &&
      !row[COL_PRICE_EUR];

    if (isAllEmpty) continue;

    const key = buildRowKey_(row);
    if (seenKeys.has(key)) continue;
    seenKeys.add(key);

    const price = toNumber_(row[COL_PRICE_EUR]);
    if (price > 0) amounts.push(price);
  }

  return amounts;
}


function testCreateDummyPdf() {
  const dummySummary = {
    totalInvoices: 5,
    totalPriceEur: 1234.56,
    totalEnergyKwh: 8800,
    totalFuelLitres: 320,
    totalCloudHours: 540,
    totalStorageGbMonth: 6500,
    totalTransferGb: 3200
  };

  const dummyNarrative =
    'This is a dummy ESG sustainability report used only for testing the PDF generation. ' +
    'The numbers here are synthetic and do not reflect real company data.';

  const year = 2025;

  const file = createEsgPdfFile_(year, dummySummary, dummyNarrative);

  if (!file) {
    Logger.log('testCreateDummyPdf: file is null');
    return;
  }

  Logger.log('testCreateDummyPdf: fileId = ' + file.getId());
  Logger.log('testCreateDummyPdf: url   = ' + file.getUrl());
}

function testGenerateEsgReport2025() {
  const year = '2025';
  const fileId = generateEsgReportForYear(year);

  Logger.log('testGenerateEsgReport2025: returned fileId = ' + fileId);
}


function testNarrativesDebug() {
  const year = 2025;

  const summary = {
    totalInvoices: 100,
    totalPriceEur: 123456.78,
    totalEnergyKwh: 9876.54,
    totalFuelLitres: 432.1,
    totalCloudHours: 88.2,
    totalStorageGbMonth: 55.7,
    totalTransferGb: 99.9
  };

  const monthlySeries = [
    { month: '2025-01', totalEnergyKwh: 100, totalPriceEur: 200 },
    { month: '2025-02', totalEnergyKwh: 120, totalPriceEur: 240 }
  ];

  const spendByCategory = [
    { category: 'ENERGY_INVOICE_ELECTRICITY', totalPriceEur: 45000 },
    { category: 'FUEL_INVOICE', totalPriceEur: 8000 }
  ];

  const invoiceAmounts = [120, 330, 550, 1200, 1800, 3000, 6500];

  Logger.log('Testing narratives…');

  const overall = buildEsgNarrativeWithSAPAI_(
    year, summary, monthlySeries, spendByCategory
  );
  Logger.log('Overall:\n' + overall);

  const ts = buildTimeSeriesNarrativeWithSAPAI_(year, monthlySeries);
  Logger.log('TimeSeries:\n' + ts);

  const cat = buildCategoryNarrativeWithSAPAI_(year, spendByCategory);
  Logger.log('Category:\n' + cat);

  const hist = buildHistogramNarrativeWithSAPAI_(year, invoiceAmounts);
  Logger.log('Histogram:\n' + hist);

  Logger.log('DONE.');
}


function testPdfDebug() {
  const year = 2025;

  const summary = {
    totalInvoices: 24,
    totalPriceEur: 12345.67,
    totalEnergyKwh: 888.123,
    totalFuelLitres: 44.567,
    totalCloudHours: 22.4,
    totalStorageGbMonth: 55.123,
    totalTransferGb: 10.55
  };

  const monthlySeries = [
    { month: '2025-01', totalEnergyKwh: 110, totalPriceEur: 220 },
    { month: '2025-02', totalEnergyKwh: 125, totalPriceEur: 250 },
    { month: '2025-03', totalEnergyKwh: 150, totalPriceEur: 300 }
  ];

  const spendByCategory = [
    { category: 'ENERGY_INVOICE_ELECTRICITY', totalPriceEur: 5000 },
    { category: 'WATER_INVOICE', totalPriceEur: 2000 },
    { category: 'FUEL_INVOICE', totalPriceEur: 1200 }
  ];

  const invoiceAmounts = [200, 320, 550, 1200, 2200, 3500, 4800];

  const overall = buildEsgNarrativeWithSAPAI_(
    year, summary, monthlySeries, spendByCategory
  );
  const ts = buildTimeSeriesNarrativeWithSAPAI_(year, monthlySeries);
  const cat = buildCategoryNarrativeWithSAPAI_(year, spendByCategory);
  const hist = buildHistogramNarrativeWithSAPAI_(year, invoiceAmounts);

  const file = createEsgPdfFile_(year, {
    summary,
    overallNarrative: overall,
    timeSeriesNarrative: ts,
    categoryNarrative: cat,
    histogramNarrative: hist,
    monthlySeries,
    spendByCategory,
    invoiceAmounts
  });

  Logger.log('PDF created: ' + (file ? file.getUrl() : 'NULL'));
}
