import os
import time

try:
    from jnius import autoclass
    PythonService = autoclass("org.kivy.android.PythonService")
    PythonService.mService.setAutoRestartService(True)
    Context = autoclass("android.content.Context")
    PowerManager = autoclass("android.os.PowerManager")
    pm = PythonService.mService.getSystemService(Context.POWER_SERVICE)
    wl = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "XauOrbBot::WakeLock")
    wl.acquire()
except Exception as e:
    print("Android wakelock/service note:", e)

import xau_orb_bot

if __name__ == "__main__":
    xau_orb_bot.start_background_threads(force=True)
    xau_orb_bot.app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False, use_reloader=False)
