# Use schemacrawler (https://www.schemacrawler.com/diagramming.html) to generate
# a PDF displaying the sqlite schema
produce_db_schema <- function(dbpath, outpath, config_file, brief = FALSE, grep_string = NULL) {

  std_args <- c(
    "-server sqlite",
    str_interp("-database ${dbpath}"),
    "-outputformat pdf",
    str_interp("-outputfile ${outpath}"),
    "-password")

  if (!is.null(grep_string)) {
    std_args <- c(std_args,
                  "-tables",
                  grep_string,
                  "-only-matching")
  }

  if (brief) {
    this_args <- c(std_args,     "-command brief", "-infolevel standard")
  } else {
    this_args <- c(std_args, "-command schema", "-infolevel maximum")
  }

  system2("schemacrawler.sh",
          args = this_args)
}

document_tables <- function(dbpath, outpath, pattern) {
  db <- dbConnect(RSQLite::SQLite(), dbpath)
  tables <- dbListTables(db) %>%
    str_subset(pattern = pattern)
  fnames <- map(set_names(tables), ~ dbListFields(db, .))
  yaml::write_yaml(fnames, file = outpath)
  dbDisconnect(db)
}
