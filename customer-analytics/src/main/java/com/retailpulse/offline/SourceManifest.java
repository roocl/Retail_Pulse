package com.retailpulse.offline;

public record SourceManifest(String source, String downloadUrl, String citation, String license,
                             String sourceSha256, String rowsSha256, String sheet, long rows,
                             String currency, String timezone, int ingestVersion) {
    public static final String DOWNLOAD_URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip";
    public static final String SHEET = "Online Retail";
}
