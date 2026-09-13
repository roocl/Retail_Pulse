package com.retailpulse.offline;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.util.XMLHelper;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStrings;
import org.apache.poi.xssf.model.StylesTable;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

public final class RetailIngestor {
    static final List<String> HEADERS = List.of("InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country");
    static final String[] FIELDS = {"source_row_id", "source_row", "content_hash", "invoice_no",
            "stock_code", "description", "quantity", "invoice_time", "unit_price", "customer_id", "country"};
    private final Path root;

    public RetailIngestor(Path root) { this.root = root.toAbsolutePath().normalize(); }

    public Path ingest(Path source) throws Exception {
        String hash = Artifacts.sha256(source);
        Path raw = Files.createDirectories(root.resolve("raw"));
        Path batch = raw.resolve(hash);
        try (var ignored = new Artifacts.Lock(raw.resolve(hash + ".lock"))) {
            if (Files.exists(batch)) {
                SourceManifest manifest = Artifacts.JSON.readValue(batch.resolve("manifest.json").toFile(), SourceManifest.class);
                if (!Artifacts.sha256(batch.resolve("source.xlsx")).equals(hash)
                        || !Artifacts.sha256(batch.resolve("rows.csv")).equals(manifest.rowsSha256()))
                    throw new IOException("Archived input checksum mismatch");
                return batch;
            }
            Path temporary = Files.createTempDirectory(raw, "ingest-");
            try {
                Path workbook = temporary.resolve("source.xlsx");
                Files.copy(source, workbook);
                if (!Artifacts.sha256(workbook).equals(hash)) throw new IOException("Source changed during import");
                long rows = read(workbook, temporary.resolve("rows.csv"), hash);
                var manifest = new SourceManifest("https://archive.ics.uci.edu/dataset/352/online+retail",
                        SourceManifest.DOWNLOAD_URL, "Chen, D. (2015). Online Retail. DOI:10.24432/C5BW33",
                        "CC BY 4.0", hash, Artifacts.sha256(temporary.resolve("rows.csv")), SourceManifest.SHEET,
                        rows, "GBP", "unspecified local wall time", 2);
                Artifacts.publish(temporary.resolve("manifest.json"), manifest);
                Files.move(temporary, batch, StandardCopyOption.ATOMIC_MOVE);
            } finally {
                if (Files.exists(temporary)) FileUtils.deleteDirectory(temporary.toFile());
            }
        }
        return batch;
    }

    private long read(Path workbook, Path csv, String hash) throws Exception {
        try (var file = OPCPackage.open(workbook.toFile(), PackageAccess.READ);
             var output = Files.newBufferedWriter(csv);
             var printer = new CSVPrinter(output, CSVFormat.DEFAULT.builder().setHeader(FIELDS).get())) {
            var reader = new XSSFReader(file);
            var sheets = (XSSFReader.SheetIterator) reader.getSheetsData();
            while (sheets.hasNext()) {
                try (var sheet = sheets.next()) {
                    if (!SourceManifest.SHEET.equals(sheets.getSheetName())) continue;
                    var handler = new Rows(reader.getSharedStringsTable(), reader.getStylesTable(), printer, hash);
                    var parser = XMLHelper.newXMLReader();
                    parser.setContentHandler(handler);
                    parser.parse(new org.xml.sax.InputSource(sheet));
                    if (handler.lastRow == 0) throw new IOException("Missing header row");
                    return handler.lastRow - 1;
                }
            }
            throw new IOException("Missing Online Retail worksheet");
        }
    }

    private static final class Rows extends DefaultHandler {
        private final SharedStrings strings;
        private final StylesTable styles;
        private final CSVPrinter printer;
        private final String hash;
        private final StringBuilder text = new StringBuilder();
        private String[] cells;
        private int row;
        private int column;
        private int style;
        private int lastRow;
        private String type;
        private boolean capture;

        Rows(SharedStrings strings, StylesTable styles, CSVPrinter printer, String hash) {
            this.strings = strings;
            this.styles = styles;
            this.printer = printer;
            this.hash = hash;
        }

        @Override public void startElement(String uri, String local, String name, Attributes attributes) throws SAXException {
            if (name.equals("row")) {
                row = Integer.parseInt(attributes.getValue("r"));
                cells = new String[8];
                Arrays.fill(cells, "");
            } else if (name.equals("c")) {
                column = new CellReference(attributes.getValue("r")).getCol();
                if (column >= 8) throw new SAXException("Unexpected Online Retail columns");
                type = attributes.getValue("t");
                style = attributes.getValue("s") == null ? 0 : Integer.parseInt(attributes.getValue("s"));
                text.setLength(0);
            } else if (name.equals("v") || name.equals("t")) {
                capture = true;
            } else if (name.equals("f")) {
                throw new SAXException("Formula cells are not supported in the source contract");
            }
        }

        @Override public void characters(char[] characters, int start, int length) {
            if (capture) text.append(characters, start, length);
        }

        @Override public void endElement(String uri, String local, String name) throws SAXException {
            if (name.equals("v") || name.equals("t")) capture = false;
            if (name.equals("c")) {
                String value = text.toString();
                if ("s".equals(type)) value = strings.getItemAt(Integer.parseInt(value)).getString();
                else if (!value.isEmpty() && (type == null || type.equals("n"))) {
                    var format = styles.getStyleAt(style);
                    if (DateUtil.isADateFormat(format.getDataFormat(), format.getDataFormatString()))
                        value = DateUtil.getLocalDateTime(Double.parseDouble(value)).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    else value = new BigDecimal(value).stripTrailingZeros().toPlainString();
                }
                cells[column] = value;
            } else if (name.equals("row")) {
                try {
                    if (row == 1) {
                        if (!HEADERS.equals(Arrays.asList(cells))) throw new IOException("Unexpected Online Retail columns");
                    } else {
                        if (lastRow == 0 || row != lastRow + 1) throw new IOException("Noncontiguous source rows");
                        String rowId = Artifacts.sha256(hash + ":" + SourceManifest.SHEET + ":" + row);
                        String contentHash = Artifacts.sha256(Artifacts.JSON.writeValueAsString(cells));
                        var fields = new java.util.ArrayList<String>(List.of(rowId, Integer.toString(row), contentHash));
                        fields.addAll(Arrays.asList(cells));
                        printer.printRecord(fields);
                    }
                    lastRow = row;
                } catch (IOException exception) { throw new SAXException(exception); }
            }
        }
    }
}
