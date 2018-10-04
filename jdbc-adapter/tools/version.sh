#!/bin/bash
readonly vs_jar_prefix='virtualschema-jdbc-adapter-dist'
readonly jar_suffix='jar'
readonly vs_jar_pattern="$vs_jar_prefix-.*\.$jar_suffix"
readonly root_dir='virtual-schemas'
readonly master_pom='jdbc-adapter/pom.xml'
readonly file_find_regex='.*\.(md|yaml)'
readonly script=$(basename $0)

main() {
    case "$1" in
        help)
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
}

usage () {
    log "Usage: $script help"
    log "       $script verify"
    log "       $script unify"
    log 
    log "Run from the root directory \"$root_dir\""
    log
    log "This script can serve as a checkpoint using 'verify' as command. The exit value"
    log "is zero when all detected version numbers match the ones on the master POM file."
    log "It is non-zero if there is a mismatch."
    log
    log "Used with the command 'unify' this script rewrites all occurrences of divergent"
    log "version numbers with the one found in the master POM file."
}

verify () {
    prepare
    verify_no_other_version_numbers "$version"
}

prepare() {
    verify_current_directory "$root_dir"
    readonly version=$(extract_product_version "$master_pom")
    log "Found version $version in master file \"$master_pom\""
}

verify_current_directory() {
    if [[ "$(basename $PWD)" != "$root_dir" ]]
    then
        log "Must be in root directory '$root_dir' to execute this script." 
        exit 1
    fi
}

extract_product_version() {
    grep -oP "product\.version>[^<]*<" "$1" | sed -e's/^.*>\s*//' -e's/\s*<//'
}

log () {
    echo "$@" 
}

verify_no_other_version_numbers() {
    find -type f -regextype posix-extended -regex "$file_find_regex" \
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
    find -type f -regextype posix-extended -regex "$file_find_regex" \
      -exec echo "Processing \"{}\"" \; \
      -exec sed -i s/"$vs_jar_pattern"/"$vs_jar_prefix-$version.$jar_suffix"/g {} \;
}

main "$@"