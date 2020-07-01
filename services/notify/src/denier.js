class Denier {
  constructor({emailBlacklist, db}) {
    this.emailBlacklist = emailBlacklist;
    this.db = db;
  }

  async isDenied(notificationType, notificationAddress) {
    if (notificationType === 'email') {
      if ((this.emailBlacklist || []).includes(notificationAddress)) {
        return true;
      }
    }

    const address = {notificationType, notificationAddress};
    let p = await this.db.fns.exists_denylist_address(address);
    let result = p[0]["exists_denylist_address"];
    return result;
  }
}

module.exports = Denier;
