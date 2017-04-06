Summary:   NetSage Resource Database
Name:      netsage-aggregate-flow-importer
Version:   1.0.0
Release:   1%{?dist}
License:   Apache
Group:     GRNOC
URL:       http://globalnoc.iu.edu

Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildArch: noarch
Requires: perl-GRNOC-Config >= 1.0.9-1
Requires: perl-GRNOC-WebService-Client >= 1.4.0-1
Requires: perl-Search-Elasticsearch
Requires: perl-List-MoreUtils
Requires: perl-JSON


%description
Application that takes flow data from ELK, aggregates it based on a circuit key, and stores
the resulting data into a TSDS instance.

%pre
/usr/bin/getent passwd netsage || /usr/sbin/useradd -r -U -s /sbin/nologin netsage

%prep
%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT

%{__install} -d -m0755 %{buildroot}/etc/netsage/aggregate-flow-importer/
%{__install} -d -m0755 %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/cron.d/

%{__install} bin/aggregate-flow-importer.pl      %{buildroot}/usr/bin/aggregate-flow-importer.pl
%{__install} conf/config.xml.example             %{buildroot}/etc/netsage/aggregate-flow-importer/config.xml
%{__install} cron/aggregate-flow-importer.cron   %{buildroot}/etc/cron.d/aggregate-flow-importer.cron


%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(755,root,root,-)
/usr/bin/aggregate-flow-importer.pl

%defattr(640,netsage,netsage,-)
%config(noreplace) /etc/netsage/aggregate-flow-importer/config.xml

%defattr(644,root,root,-)
%config(noreplace) /etc/cron.d/aggregate-flow-importer.cron

