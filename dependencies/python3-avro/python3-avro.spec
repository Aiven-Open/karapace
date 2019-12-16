Name: python3-avro
Summary: Python interface for Avro serialization and RPC framework
Version: %{upstream_version}
Release: %{karapace_release}%{?dist}
License: ASL 2.0
Group: Development/Libraries
Source0: python3-avro-%{version}.tar.gz
BuildArch: noarch
Epoch: 1000
Url: https://github.com/apache/avro

%description
%{summary}


%prep
%setup

%build
cd lang/py3 && python3 setup.py build

%install
cd lang/py3 && python3 setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*

%clean
rm -rf $RPM_BUILD_ROOT

%files -f lang/py3/INSTALLED_FILES
%defattr(-,root,root)
