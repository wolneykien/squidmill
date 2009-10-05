Name: squidmill
Version: 2.0
Release: alt3

Source: %name-%version.tar.gz

Packager: Paul Wolneykien <manowar@altlinux.ru>

Summary: Squid access log file processing utility
License: GPL
Group: System/Configuration/Other

# Automatically added by buildreq on Mon Apr 20 2009
BuildRequires: gambit gambit-sqlite3-devel gambit-signal-devel rpm-macros-fillup

%description
Squidmill unility can acquire and integrate information from
Squid proxy server access log files. Rounding function can be used to
save space (and reporting time).

Online database update service and anacron job file for rounding an old
data are included.

%prep
%setup

%build
%make includedir=%{_includedir} libdir=%{_libdir}

%install
%makeinstall initdir=%buildroot%{_initdir}

%files
%_sbindir/squidmill
%_sysconfdir/cron.daily/squidmill
%_initdir/squidmill

%changelog
* Thu Oct 01 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt3
- Fix error in Scheme output to stdout.
- Exit with exit code 100 iff reporting limit is exceeded.
- Use glob patterns.
- Summary action added.

* Thu Oct 01 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt2
- Squidmill service update: create DB file directory if necessary.
- Fix errors in the daily anacron job.

* Tue Sep 29 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt1
- SQLite3 based version.
- Online database update service.

* Fri Aug 28 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt3
- Fix stdin reading in the daily squidmill script.

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt2
- Using new gsc compiler (new name).

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt1
- Initial release.
