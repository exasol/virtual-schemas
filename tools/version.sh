#!/bin/bash
readonly vs_jar_prefix='virtual-schema-dist'
readonly jar_suffix='jar'
readonly vs_jar_pattern="$vs_jar_prefix-.*\.$jar_suffix"
readonly root_dir='virtual-schemas'
readonly master_pom='pom.xml'
readonly file_find_regex='.*\.(md|yaml|java)'
readonly script=$(basename $0)

main() {
    if [[ "$#" -eq 0 ]]
    then
        extract_product_version "$master_pom"
    else
        case "$1" in
            help | '-h' | '--help' | usage)
                usage
                ;;
            verify)
                verify
                ;;
            unify)
                unify
                ;;
            *)
                log "Unknown command: \"$1\""
                log
                usage
                exit 1
                ;;
        esac
    fi
}

usage () {
    log "Usage: $script"
    log "       $script help | -h | --help | usage"
    log "       $script verify"
    log "       $script unify"
    log
    log "Run from the root directory \"$root_dir\""
    log
    log "Running the script without parameter returns the current version from the main"
    log "POM file."
    log
    log "This script can serve as a checkpoint using 'verify' as command. The exit value"
    log "is zero when all detected version numbers match the ones on the main POM file."
    log "It is non-zero if there is a mismatch."
    log
    log "Used with the command 'unify' this script rewrites all occurrences of divergent"
    log "version numbers with the one found in the master POM file."
}

verify () {
    prepare
    verify_no_other_version_numbers "$dialects_version"
}

prepare() {
    verify_current_directory "$root_dir"
    readonly vscjdbc_version=$(extract_vscjdbc_version "$master_pom")
    log "Found virtual-schema-common-java version $vscjdbc_version in master file \"$master_pom\""
    readonly dialects_version=$(extract_product_version "$master_pom")
    log "Found project version $dialects_version in master file \"$master_pom\""
}

verify_current_directory() {
    if [[ "$(basename $PWD)" != "$root_dir" ]]
    then
        log "Must be in root directory '$root_dir' to execute this script."
        exit 1
    fi
}

extract_vscjdbc_version() {
    grep -oP "<vscjdbc.version>[^<]*<" "$1" | head -n1 | sed -e's/^.*>\s*//' -e's/\s*<//'
}

extract_product_version() {
    grep -oP "<version>[^<]*<" "$1" | head -n1 | sed -e's/^.*>\s*//' -e's/\s*<//'
}

log () {
    echo "$@"
}

verify_no_other_version_numbers() {
    find -type f -regextype posix-extended -regex "$file_find_regex" -not -path "./doc/changes/*" \
      -exec grep -Hnor $vs_jar_pattern {} \; | grep -v "$1"
    if [[ $? -eq 0 ]]
    then
        log
        log "Verification failed."
        log "Found version mismatches that need to be fixed. Try the following command"
        log
        log "    $script unify"
        exit 1
    else
        log "Verification successful."
    fi
}

unify() {
    prepare
    update_documentation
}

update_documentation() {
log "Checking all files matching \"$file_find_regex\""
    find -type f -regextype posix-extended -regex "$file_find_regex" -not -path "./doc/changes/*" \
      -exec echo "Processing \"{}\"" \; \
      -exec sed -i s/"$vs_jar_pattern"/"$vs_jar_prefix-$dialects_version.$jar_suffix"/g {} \; \
      -exec sed -i s/"$vs_jar_pattern"/"$vs_jar_prefix-$vscjdbc_version-bundle-$dialects_version.$jar_suffix"/g {} \;
}

main "$@"
