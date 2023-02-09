Compare rsync and clean_path:

rsync: `clean_fname | if (module_dir.len() > 0) { sanitize_path }`

clean_fname:
1. turns multiple adjacent slashes into a single slash (possible exception: the
preserving of two leading slashes at the start)
2. drops all leading or interior "." elements
3. drop a trailing '.' after a '/'
4. removes a trailing slash
5. collapse ".." elements (suppressed by sanitize_path: except at the start)
6. if the resulting name would be empty, returns "."

sanitize_path:
1. handles a leading "/" (either removing it or expanding it) and any leading or embedded
".." components that attempt to escape past the module's top dir.
2. turns multiple adjacent slashes into a single slash
3. gets rid of "." dir elements (INCLUDING a trailing dot dir)
4. PRESERVES a trailing slash (suppressed by clean_fname)
5. ALWAYS collapses ".." elements.
6. if the resulting name would be empty, change it into a ".".

clean_path:
1. Reduce multiple slashes to a single slash. (~=1, but won't // at the start)
2. Eliminate . path name elements (the current directory). (=2,3)
3. Eliminate .. path name elements (the parent directory) and the non-. non-.., element
that precedes them. (+5=5)
4. Eliminate .. elements that begin a rooted path, that is, replace /.. by / at the
beginning of a path.
5. Leave intact .. elements that begin a non-rooted path. (+3=5)
6. remove trailing slash (=4)
7. If the result of this process is an empty string, return the string "." (=6)

Major differences:
1. does not sanitize path, but there's no security issue here because we don't use fs
2. does not preserve // at the start: some POSIX system treats "//*" and "/*" as different
paths, which is rare in mirror use case.
