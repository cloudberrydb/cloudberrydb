#ifndef SENDALERT_COMMON_H
#define SENDALERT_COMMON_H

int set_alert_severity(const GpErrorData * errorData,
	char *subject,
	bool *send_via_email,
	char *email_priority,
	bool *send_via_snmp,
	char *snmp_severity);


#endif
