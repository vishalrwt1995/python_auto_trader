#!/usr/bin/env node
/**
 * Sets the 'admin' custom claim on a Firebase user.
 * Usage: node set-admin-claim.js <email>
 *
 * Requires: GOOGLE_APPLICATION_CREDENTIALS or running on GCP with default credentials.
 * Install: npm install firebase-admin
 */
const admin = require("firebase-admin");

admin.initializeApp({
  projectId: "grow-profit-machine",
});

const email = process.argv[2];
if (!email) {
  console.error("Usage: node set-admin-claim.js <email>");
  process.exit(1);
}

(async () => {
  try {
    const user = await admin.auth().getUserByEmail(email);
    await admin.auth().setCustomUserClaims(user.uid, { role: "admin" });
    console.log(`Set admin claim on ${email} (uid: ${user.uid})`);
    console.log("User must sign out and sign back in for claims to take effect.");
  } catch (err) {
    console.error("Error:", err.message);
    process.exit(1);
  }
})();
