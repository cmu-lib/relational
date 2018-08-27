# SQL Export helpers ----

handle_column <- function(name, type, is_p, is_u, is_nn) {
  p <- if_else(is_p, " PRIMARY KEY UNIQUE NOT NULL", "")
  u <- if_else(is_u, " UNIQUE", "")
  nn <- if_else(is_nn, " NOT NULL", "")
  str_interp("${name} ${type}${p}${u}${nn}")
}

format_pf_key <- function(db, df, tbl_name, p_key, u_keys, nn_keys, f_keys) {

  cnames <-  names(df)
  ctypes <- map_chr(df, dbDataType, db = db)

  stopifnot(p_key %in% cnames)
  stopifnot(all(u_keys %in% cnames))
  stopifnot(all(nn_keys %in% cnames))

  if (is.null(p_key)) {
    prefix_column <- "\trowid INTEGER PRIMARY KEY,\n"
  } else {
    prefix_column <- NULL
  }

  all_fields <- map2_chr(cnames, ctypes, function(name, type) {
    handle_column(name, type,
                  is_p = name %in% p_key,
                  is_u = name %in% u_keys,
                  is_nn = name %in% nn_keys)
  }) %>%
    paste0("\t", ., collapse = ",\n")

  all_fields <- str_c(prefix_column, all_fields)

  foreign_key <- ""
  if (!is.null(f_keys)) {
    foreign_key <- map_chr(f_keys, function(x) {
      f_key <- x$f_key
      parent_tbl_name <- x$parent_tbl_name
      parent_f_key <- x$parent_f_key
      str_interp("\tFOREIGN KEY (${f_key}) REFERENCES ${parent_tbl_name}(${parent_f_key})")
    }) %>%
      paste0(collapse = ",\n") %>%
      paste0(",\n", .)
  }

  str_interp("CREATE TABLE ${tbl_name}\n(\n${all_fields}${foreign_key}\n)")
}

# Builds indexes on foreign keys
build_indexes <- function(tbl_name, f_keys) {
  index_calls <- f_keys %>%
    map("f_key") %>%
    map_chr(function(key) str_interp("CREATE INDEX index_${tbl_name}_${key} on ${tbl_name}(${key})"))
  walk(index_calls, message)
  index_calls
}

write_tbl_key <- function(db, df, tbl_name, p_key = NULL, u_keys = NULL, nn_keys = NULL, no_null = FALSE, f_keys = NULL) {
  if (no_null)
    nn_keys <- names(df)

  command <- format_pf_key(db, df, tbl_name, p_key, u_keys, nn_keys, f_keys)
  message(command)
  dbExecute(db, command)
  build_indexes(tbl_name, f_keys) %>%
    walk(~ dbExecute(conn = db, .x))
  dbWriteTable(db, tbl_name, df, append = TRUE, overwrite = FALSE)
}

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

db_setup <- function(dbpath) {
  db <- dbConnect(RSQLite::SQLite(), dbpath)
  return(db)
}

db_enable_foreign_key_check <- function(db) {
  # Enforce foreign key constraints
  dbExecute(db, "PRAGMA foreign_keys = ON")
  stopifnot(dbGetQuery(db, "PRAGMA foreign_keys")[["foreign_keys"]][1] == 1)
}

db_disable_foreign_key_check <- function(db) {
  # Enforce foreign key constraints
  dbExecute(db, "PRAGMA foreign_keys = OFF")
  stopifnot(dbGetQuery(db, "PRAGMA foreign_keys")[["foreign_keys"]][1] == 0)
}

# Cleanup the db before finishing
db_cleanup <- function(db, vacuum = FALSE) {

  message("Validating foreign keys...")
  fk_problems <- dbGetQuery(db, "PRAGMA foreign_key_check")
  n_fk_problems <- nrow(fk_problems)

  if (n_fk_problems > 0) {
    message(n_fk_problems, " foreign key violations found and saved to fk_problems table.")
    dbWriteTable(db, "fk_problems", fk_problems)
    fk_problems %>%
      count(table, parent) %>%
      as_tibble() %>%
      print()
  } else {
    message("... all valid!")
  }

  if (vacuum) db_vacuum(db)

  dbDisconnect(db)
}

db_vacuum <- function(db) {
  message("Vacuuming database...")
  dbExecute(db, "VACUUM")
}

db_clear <- function(db, pattern) {

  tblnames <- dbGetQuery(db, "SELECT name FROM sqlite_master WHERE type='table'")[["name"]] %>%
    str_subset(pattern = pattern)

  walk(tblnames, function(x) {
    command <- str_interp("DROP TABLE IF EXISTS ${x}")
    message(command)
    dbExecute(db, command)
  })
}

document_tables <- function(dbpath, outpath, pattern) {
  db <- dbConnect(RSQLite::SQLite(), dbpath)
  tables <- dbListTables(db) %>%
    str_subset(pattern = pattern)
  fnames <- map(set_names(tables), ~ dbListFields(db, .))
  yaml::write_yaml(fnames, file = outpath)
  dbDisconnect(db)
}
