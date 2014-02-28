#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import suds #sudo easy_install suds
url = 'http://10.12.32.63:8080/maint/services/SiteRouterService?wsdl'
client = suds.client.Client(url)
print client
result =  client.service.getSiteRouterInfo("www.test.com")  #这个号码是办证的，拿来测试，哈哈
print result
print client['siteCode']