[app]
title = XAU ORB Bot
package.name = xauorbbot
package.domain = org.tarek
source.dir = .
source.include_exts = py,png,jpg,kv,atlas,json
version = 1.0
requirements = python3,kivy,flask,werkzeug,jinja2,markupsafe,itsdangerous,click,blinker,requests,urllib3,certifi,idna,charset-normalizer,pyjnius
orientation = portrait
fullscreen = 0
android.permissions = INTERNET,WAKE_LOCK,FOREGROUND_SERVICE,FOREGROUND_SERVICE_DATA_SYNC,POST_NOTIFICATIONS,RECEIVE_BOOT_COMPLETED
android.api = 33
android.minapi = 24
android.archs = arm64-v8a
android.allow_backup = True
services = Orbservice:service.py:sticky

[buildozer]
log_level = 2
warn_on_root = 1
