const debug = require('debug')('notify');
const _ = require('lodash');
const path = require('path');
const crypto = require('crypto');
const marked = require('marked');
const Email = require('email-templates');
const nodemailer = require('nodemailer');

/**
 * Object to send notifications, so the logic can be re-used in both the pulse
 * listener and the API implementation.
 */
class Notifier {
  constructor(options = {}) {
    // Set default options
    this.options = _.defaults({}, options, {
      emailBlacklist: [],
    });
    this.hashCache = [];
    this.publisher = options.publisher;
    this.rateLimit = options.rateLimit;
    this.queueName = this.options.queueName;
    this.sender = options.sourceEmail;

    const transport = nodemailer.createTransport({
      SES: options.ses,
    });
    this.emailer = new Email({
      transport,
      send: true,
      preview: false,
      views: {root: path.join(__dirname, 'templates')},
      juice: true,
      juiceResources: {
        webResources: {
          relativeTo: path.join(__dirname, 'templates'),
        },
      },
    });
  }

  key(idents) {
    return crypto
      .createHash('md5')
      .update(JSON.stringify(idents))
      .digest('hex');
  }

  isDuplicate(...idents) {
    return _.indexOf(this.hashCache, this.key(idents)) !== -1;
  }

  markSent(...idents) {
    this.hashCache.unshift(this.key(idents));
    this.hashCache = _.take(this.hashCache, 1000);
  }

  async email({address, subject, content, link, replyTo, template}) {
    if (this.isDuplicate(address, subject, content, link, replyTo)) {
      debug('Duplicate email send detected. Not attempting resend.');
      return;
    }

    // Don't notify emails on the denylist
    if (this.options.emailBlacklist.includes(address)) {
      debug('Denylist email: %s send detected, discarding the notification', address);
      return;
    }

    const rateLimit = this.rateLimit.remaining(address);
    if (rateLimit <= 0) {
      debug('Ratelimited email: %s is over its rate limit, discarding the notification', address);
      return;
    }

    debug(`Sending email to ${address}`);
    // It is very, very important that this uses the sanitize option
    let formatted = marked(content, {
      gfm: true,
      tables: true,
      breaks: true,
      pedantic: false,
      sanitize: true,
      smartLists: true,
      smartypants: false,
    });

    const res = await this.emailer.send({
      message: {
        from: this.sender,
        to: address,
      },
      template: template || 'simple',
      locals: {address, subject, content, formatted, link, rateLimit},
    });
    this.rateLimit.markEvent(address);
    this.markSent(address, subject, content, link, replyTo);
    return res;
  }

  async pulse({routingKey, message}) {
    if (this.isDuplicate(routingKey, message)) {
      debug('Duplicate pulse send detected. Not attempting resend.');
      return;
    }
    debug(`Publishing message on ${routingKey}`);
    return this.publisher.notify({message}, [routingKey]).then(res => {
      this.markSent(routingKey, message);
      return res;
    });
  }

  async irc(msg) {
    const {channel, user, message} = msg;
    if (channel && !/^[#&][^ ,\u{0007}]{1,199}$/u.test(channel)) {
      debug('irc channel ' + channel + ' invalid format. Not attempting to send.');
      return;
    }
    if (this.isDuplicate(channel, user, message)) {
      debug('Duplicate irc message send detected. Not attempting resend.');
      return;
    }

    debug(`Publishing message on irc for ${user || channel}.`);
    return this.publisher.ircNotify({message: msg}, ['irc']).then(res => {
      this.markSent(channel, user, message);
      return res;
    });
  }
}

// Export notifier
module.exports = Notifier;
