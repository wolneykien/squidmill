Name: squidmill
Version: 2.4
Release: alt1

Source: %name-%version.tar

Packager: Paul Wolneykien <manowar@altlinux.ru>

Summary: Squid proxy server access log collector with rounding support
License: GPLv3+
Group: System/Configuration/Other

BuildRequires: gambit
BuildRequires: gambit-sqlite3-devel >= 1.2-alt7
BuildRequires: gambit-signal-devel >= 1.1-alt1
BuildRequires: gambit-dsock-devel >= 1.1-alt1
BuildRequires: rpm-macros-fillup sqlite3 /usr/bin/dc

%description
Squidmill daemon acquires and integrates information from a
Squid proxy server access log files. Rounding is supported to
save space and reporting time.

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
* Thu Jun 27 2013 Paul Wolneykien <manowar@altlinux.org> 2.4-alt1
- Update the program internal version number.
- Bulk insert without an explicit transaction (faster!).
- Server socket for DB-file, client otherwise.
- Require gambit-signal >= 1.1.
- Require gambit-dsock >= 1.1.
- Check and round the existing data before inserting the new data.
- Lock the DB-mutex over the whole transaction. Select data with
  no explicit transaction.
- Use MAXRECORDS configuration parameter to specify the rounding size.
- Round every N rows not minutes.
- Add option for a log-file.
- Update the license and the description.
- Use bulk size of 1 by default.
- Make the checks when building the package.
- Implement the SQL-server.
- Remove the DB-reopen stuff.

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
