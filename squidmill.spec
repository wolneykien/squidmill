Name: squidmill
Version: 2.3
Release: alt1

Source: %name-%version.tar

Packager: Paul Wolneykien <manowar@altlinux.ru>

Summary: Squid access log file processing utility
License: GPL
Group: System/Configuration/Other

# Automatically added by buildreq on Mon Apr 20 2009
BuildRequires: gambit gambit-sqlite3-devel gambit-signal-devel gambit-dsock-devel rpm-macros-fillup

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
%makeinstall initdir=%buildroot%{_initdir} unitdir=%buildroot%_unitdir

%check
%make check

%preun
%preun_service squidmill

%files
%_sbindir/squidmill
%_initdir/squidmill
%_unitdir/squidmill.service
%_sysconfdir/sysconfig/squidmill

%changelog
* Tue May 14 2013 Paul Wolneykien <manowar@altlinux.ru> 2.3-alt1
- Make use of the rounding period value in the service files.
- Add configuration option for rounding period, 1440 min by default.
- Rounding period of 0 means no rounding.
- Remove the anacron daily job.
- Implement in-process rounding.

* Mon Apr 08 2013 Paul Wolneykien <manowar@altlinux.ru> 2.2-alt4
- Rebuild with a new version of Gambit.

* Fri Jan 04 2013 Paul Wolneykien <manowar@altlinux.ru> 2.2-alt3
- Rebuild with Gambit v4.6.6.

* Thu Nov 29 2012 Paul Wolneykien <manowar@altlinux.ru> 2.2-alt2
- Add the systemd unit file and configuration (environment)
  file (closes: 28087).

* Tue Mar 13 2012 Paul Wolneykien <manowar@altlinux.ru> 2.2-alt1
- Use "sqlite_master" table to query for table existence.
- Reopen the DB in the case of DB logic error.
- Retry any statement on DB busy including commit and rollback.

* Tue Nov 08 2011 Paul Wolneykien <manowar@altlinux.ru> 2.1-alt2
- Report exceptions to the stdandard error port.

* Thu Oct 27 2011 Paul Wolneykien <manowar@altlinux.ru> 2.1-alt1
- Read the default access_log value if it isn't set.

* Wed Dec 23 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt5
- Use immediate transactions (closes: 22606)

* Fri Oct 09 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt4
- Use preun_service macro.

* Thu Oct 01 2009 Paul Wolneykien <manowar@altlinux.ru> 2.0-alt3
- Fix error in Scheme output to stdout.
- Exit with exit code 100 iff reporting limit is exceeded.
- Use glob patterns.
- Summary action added.
- Pre-uninstall script: stop the service and remove it from startup
  configuration

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
