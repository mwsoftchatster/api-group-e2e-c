/* jshint esnext: true */
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-group-e2e-c/config/config.js');
var nodemailer = require('nodemailer');


/**
 * setup the nodemailer
 * create reusable transporter object using the default SMTP transport
 * 
 */
let transporter = nodemailer.createTransport({
    host: config.email.host,
    port: config.email.port,
    secure: config.email.secure,
    auth: {
        user: config.email.auth.user,
        pass: config.email.auth.pass
    }
});


/*
 * Sends email containing generated error
 * 
 */
module.exports.sendApiGroupE2ECErrorEmail = function (error) {
  var mailOptions = {
      from: '"Chatster" <mwsoft01@mwsoft.nl>', // sender address
      to: 'n.karpovas@yahoo.com', // list of receivers
      subject: 'Chatster Api Group E2E C Error', // Subject line
      text: `Chatster Group E2E C Error`, // plain text body
      html: `<p>The following error has been generated:</p> <p>${error}</p>` // html body
  };
  // send mail with defined transport object
  transporter.sendMail(mailOptions, (error, info) => {
      if (error) {
          // console.log(error);
      }
  });
}


/*
 * Sends an email to notify of successfull startup of this service
 * 
 */
module.exports.sendNewApiGroupE2ECIsUpEmail = function () {
  var mailOptions = {
      from: '"Chatster" <mwsoft01@mwsoft.nl>', // sender address
      to: 'n.karpovas@yahoo.com', // list of receivers
      subject: 'Chatster New Api Group E2E C Server Is Up', // Subject line
      text: `Chatster New Api Group E2E C Server Is Up`
  };
  // send mail with defined transport object
  transporter.sendMail(mailOptions, (error, info) => {
      if (error) {
          // console.log(error);
      }
  });
}

