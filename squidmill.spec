Name: squidmill
Version: 1.0
Release: alt3

Source: %name-%version.tar.gz
Source1: %name-%version-jobs.tar.gz

Packager: Paul Wolneykien <manowar@altlinux.ru>

Summary: Squid access log file processing utility
License: GPL
Group: System/Configuration/Other

# Automatically added by buildreq on Mon Apr 20 2009
BuildRequires: gambit rpm-macros-fillup

%description
Squidmill unility can retrive and integrate information from both
Squid proxy server access log files and its own output
(a Scheme-expression).

Anacron job files for daily, weekly, monthly and summary statistics
maintaince are included.

%prep
%setup -b1

%build
gsc -link squidmill && \
gcc -o squidmill squidmill.c squidmill_.c -lgambc

%install
install -p -m0755 -D squidmill %buildroot%_sbindir/squidmill
install -p -m0755 -D ../%name-%version-jobs/daily %buildroot%_sysconfdir/cron.daily/squidmill
install -p -m0755 -D ../%name-%version-jobs/weekly %buildroot%_sysconfdir/cron.weekly/squidmill
install -p -m0755 -D ../%name-%version-jobs/monthly %buildroot%_sysconfdir/cron.monthly/squidmill

%files
%_sbindir/squidmill
%_sysconfdir/cron.daily/squidmill
%_sysconfdir/cron.weekly/squidmill
%_sysconfdir/cron.monthly/squidmill

%changelog
* Fri Aug 28 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt3
- Fix stdin reading in the daily squidmill script.

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt2
- Using new gsc compiler (new name).

* Mon Apr 20 2009 Paul Wolneykien <manowar@altlinux.ru> 1.0-alt1
- Initial release.
