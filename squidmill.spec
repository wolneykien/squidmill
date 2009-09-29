Name: squidmill
Version: 2.0
Release: alt1

Source: %name-%version.tar.gz

Packager: Paul Wolneykien <manowar@altlinux.ru>

Summary: Squid access log file processing utility
License: GPL
Group: System/Configuration/Other

# Automatically added by buildreq on Mon Apr 20 2009
BuildRequires: gambit rpm-macros-fillup

%description
Squidmill unility can acquire and integrate information from
Squid proxy server access log files. Rounding function can be used to
save space (and reporting time).

Online database update service and anacron job file for rounding an old
data are included.

%prep
%setup -b1

%build
gsc -:daq- -link %{_includedir}/gambit/libgambc-sqlite3.c squidmill.scm
gsc -:daq- -obj squidmill.c squidmill_.c
gcc squidmill.o squidmill_.o -Wl,-rpath,%{_libdir}/gambit -lgambc -L%{_libdir}/gambit -lgambc-sqlite3 -o squidmill

%install
install -p -m0755 -D squidmill %buildroot%_sbindir/squidmill
install -p -m0755 -D squidmill-daily %buildroot%_sysconfdir/cron.daily/squidmill
install -p -m0755 -D squidmill-service %buildroot%_initdir/squidmill

%files
%_sbindir/squidmill
%_sysconfdir/cron.daily/squidmill
%_initdir/squidmill

%changelog
* Tue Sep 29 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt1
- SQLite3 based version.
- Online database update service.

* Fri Aug 28 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt3
- Fix stdin reading in the daily squidmill script.

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt2
- Using new gsc compiler (new name).

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt1
- Initial release.
