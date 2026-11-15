import os
import time
import threading

try:
    from jnius import autoclass
    ANDROID = True
except Exception:
    ANDROID = False

import xau_orb_bot


def start_android_service():
    if not ANDROID:
        return
    try:
        mActivity = autoclass("org.kivy.android.PythonActivity").mActivity
        context = mActivity.getApplicationContext()
        service_name = str(context.getPackageName()) + ".ServiceOrbservice"
        service_cls = autoclass(service_name)
        service_cls.start(mActivity, "")
    except Exception as e:
        print("Service start note:", e)


if __name__ == "__main__":
    if ANDROID:
        start_android_service()
        # Keep main WebView process alive while service serves 127.0.0.1:8080
        while True:
            time.sleep(60)
    else:
        xau_orb_bot.start_background_threads(force=True)
        xau_orb_bot.app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
