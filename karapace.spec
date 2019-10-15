Name:           karapace
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/karapace
Summary:        Your Kafka essentials in one tool
License:        ASL 2.0
Source0:        karapace-rpm-src.tar
BuildArch:      noarch
BuildRequires:  python3-aiohttp
BuildRequires:  python3-aiosocksy
BuildRequires:  python3-avro
BuildRequires:  python3-devel
BuildRequires:  python3-flake8
BuildRequires:  python3-isort
BuildRequires:  python3-kafka
BuildRequires:  python3-pylint
BuildRequires:  python3-pytest
BuildRequires:  python3-requests
BuildRequires:  python3-yapf
Requires:       python3-aiohttp
Requires:       python3-aiosocksy
Requires:       python3-avro
Requires:       python3-kafka
Requires:       python3-requests
Requires:       systemd

%undefine _missing_build_ids_terminate_build

%description
Your Kafka essentials in one tool


%prep
%setup -q -n karapace


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
rm -rf %{buildroot}%{python3_sitelib}/tests/
%{__install} -Dm0644 karapace.unit %{buildroot}%{_unitdir}/karapace.service
%{__mkdir_p} %{buildroot}%{_localstatedir}/lib/karapace


%check
make -j flake8 pylint

%files
%defattr(-,root,root,-)
%doc LICENSE README.rst karapace.config.json
%{_bindir}/karapace*
%{_unitdir}/karapace.service
%{python3_sitelib}/*


%changelog
* Mon Jan 14 2019 Hannu Valtonen <hannu.valtonen@aiven.io> - 0.0.1
- Initial RPM package
