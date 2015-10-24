#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <getopt.h>

#include <signal.h>
#include <errno.h>

#include <sys/mount.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

static int agent_main(int argc, char **argv);

static int storagegroup_main(int argc, char **argv);

static void parse_target_dev(char *input, char **WWN, char **SP, char **SG_NAME, char **ARGS, char **FSTYPE);
static int match(char **p, char **val, const char *token);

static int emc_fencer(const char *sp_name, const char *sg_name, const char *host, int unfence);

static
void usage(const char *exec)
{
	fprintf(stderr,
			"%s: \n"
			"   agent mode\n"
			"    --agent -t <type> -a <active> -p <primary host> -d <primary device> -m <primary mountpoint>\n"
			"    -q <mirror host> -e <mirror device> -n <mirror mountpoint>\n", exec);
	exit(1);
}

/*
 * We don't allow any characters which are shell escapes.
 *
 * Obviously this isn't perfect security for our setuid-root system() call,
 * but it is better than nothing.
 */
static int
arg_safe(char *input, int len)
{
	int i;

	for (i=0; i < len; i++)
	{
		switch (input[i])
		{
			case '(':
			case ')':
			case '[':
			case ']':
			case '*':
			case '~':
			case '>':
			case '<':
			case '\t':
			case '`':
			case '|':
			case '$':
			case '\\':
			case '&':
			case ';':
				return 0;
			default:
				/* nothing to do here. */
				continue;
		}
	}

	return 1;
}

static char **
opt_strip_first(int argc, char **argv)
{
	int i;
	char **r = NULL;

	r = (char **)malloc(sizeof(char *) * (argc - 1));
	if (r == NULL)
	{
		fprintf(stderr, "option allocation failed.\n");
		usage(argv[0]);
		/* not reached */
	}

	r[0] = argv[0];
	for (i=2; i < argc; i++)
	{
		r[i - 1] = argv[i];
	}

	return r;
}

int
main(int argc, char **argv)
{
	char **stripped=NULL;

	if (argc < 2)
	{
		/* not reached */
		usage(argv[0]);
	}

	stripped = opt_strip_first(argc, argv);

	if (strcmp(argv[1], "--agent") == 0)
	{
		agent_main(argc-1, stripped);
	}
	else if (strcmp(argv[1], "--storagegroup") == 0)
	{
		storagegroup_main(argc-1, stripped);
	}
	else
	{
		usage(argv[0]);
		/* not reached */
	}

	exit(0);
}

static int
storagegroup_main(int argc, char **argv)
{
	struct stat stat_buf;
	char cmd[1000];

	sprintf(cmd, "%s display dev=all", argv[1]);

	fprintf(stdout, "command %s\n", cmd);

	/*
	 * Do a quick&dirty security check before exec()ing anything
	 * ... note this is obviously not comprehensive: we depend on no
	 * one changing the file out from under us between the stat and
	 * exec.
	 */

	if (lstat(argv[1], &stat_buf) < 0)
	{
		fprintf(stderr, "Cannot lstat %s\n", argv[1]);
		exit(1);
	}

	if (S_ISLNK(stat_buf.st_mode))
	{
		fprintf(stderr, "Cannot use %s: it is a symbolic link", argv[1]);
		exit(1);
	}

	if (stat(argv[1], &stat_buf) < 0)
	{
		fprintf(stderr, "Cannot stat %s\n", argv[1]);
		exit(1);
	}

	if (stat_buf.st_uid != (uid_t)0)
	{
		fprintf(stderr, "Cannot use %s: invalid ownership\n", argv[1]);
		exit(1);
	}

	if (((stat_buf.st_mode & S_IWGRP) != 0) || ((stat_buf.st_mode & S_IWOTH) != 0))
	{
		fprintf(stderr, "Cannot use %s: invalid permissions\n", argv[1]);
		exit(1);
	}

	/*
	 * We're now *really* taking ourselves into never-never land:
	 * go get a real root uid.
	 */
	setuid(0);

	execl(argv[1], argv[1], "display", "dev=all", NULL);

	/* not reached */
	return(1);
}

static int
agent_main(int argc, char **argv)
{
	int			opt, bad_opts=0;
	int			u_mount=0;
	char		type=0;
	char		active=0;
	char		*p_host=NULL, *m_host=NULL;
	char		*p_dev=NULL, *m_dev=NULL;
	char		*p_mp=NULL, *m_mp=NULL;

	char cmd[1000];
	char *target_host=NULL, *target_dev=NULL, *target_mountpoint=NULL;

	char *old_host=NULL, *old_mountpoint=NULL;
	int status;

	while ((opt = getopt(argc, argv, "uht:a:p:d:m:q:e:n:")) != -1)
	{
		switch(opt)
		{
			case 'u':
				u_mount = 1;
				break;
			case 't':
				/* type 'n' or 'e' */
				if (optarg == NULL || strlen(optarg) > 1)
				{
					fprintf(stderr, "Invalid type\n");
					usage(argv[0]);
				}

				type = *optarg;
				if (type == 'n')
					fprintf(stdout, "type is NFS\n");
				else if (type == 'e')
					fprintf(stdout, "type is EMC\n");
				else
				{
					fprintf(stderr, "Invalid type\n");
					usage(argv[0]);
				}

				break;
			case 'a':
				/* active indicator 'p' or 'm' */
				if (optarg == NULL || strlen(optarg) > 1)
				{
					fprintf(stderr, "Invalid active\n");
					usage(argv[0]);
				}

				active = *optarg;
				if (active == 'p')
					fprintf(stdout, "active is primary\n");
				else if (active == 'm')
					fprintf(stdout, "active is mirror\n");
				else
				{
					fprintf(stderr, "Invalid active\n");
					usage(argv[0]);
				}
				break;
			case 'p':
				/* primary hostname */
				p_host = optarg;
				break;
			case 'd':
				/* primary device-id */
				p_dev = optarg;
				break;
			case 'm':
				/* primary mount-point */
				p_mp = optarg;
				break;
			case 'q':
				/* mirror host */
				m_host = optarg;
				break;
			case 'e':
				/* mirror device-id */
				m_dev = optarg;
				break;
			case 'n':
				/* mirror mount-point */
				m_mp = optarg;
				break;
			case 'h':
			default:
				usage(argv[0]);
				/* not reached */
				break;
		}
	}

	if (type == 0)
	{
		fprintf(stderr, "Bad type\n");
		bad_opts++;
	}

	if (active == 0)
	{
		fprintf(stderr, "Bad active\n");
		bad_opts++;
	}

	if (p_host == NULL || m_host == NULL)
	{
		fprintf(stderr, "Bad host\n");
		bad_opts++;
	}

	if (p_dev == NULL || m_dev == NULL)
	{
		fprintf(stderr, "Bad device\n");
		bad_opts++;
	}

	if (p_mp == NULL || m_mp == NULL)
	{
		fprintf(stderr, "Bad mountpoint\n");
		bad_opts++;
	}

	if (bad_opts != 0)
	{
		usage(argv[0]);
		/* not reached */
	}

	fprintf(stdout, "type %c active %c primary %s:'%s':'%s' mirror %s:'%s':'%s'\n",
			type, active, p_host, p_dev, p_mp, m_host, m_dev, m_mp);

	if (active == 'p')
	{
		target_host = m_host;
		target_dev = m_dev;
		target_mountpoint = m_mp;

		old_host = p_host;
		old_mountpoint = p_mp;
	}
	else
	{
		target_host = p_host;
		target_dev = p_dev;
		target_mountpoint = p_mp;

		old_host = m_host;
		old_mountpoint = m_mp;
	}

	fprintf(stdout, "agent_main: p_host '%s' p_mp '%s' m_host '%s' m_mp '%s'\n", p_host, p_mp, m_host, m_mp);

	if (!arg_safe(target_host, strlen(target_host)))
	{
		fprintf(stderr, "invalid host string [%s]\n", target_host);
		exit(1);
	}

	if (!arg_safe(old_host, strlen(old_host)))
	{
		fprintf(stderr, "invalid host string [%s]\n", old_host);
		exit(1);
	}

	if (!arg_safe(target_mountpoint, strlen(target_mountpoint)))
	{
		fprintf(stderr, "invalid mountpoint string [%s]\n", target_mountpoint);
		exit(1);
	}

	if (!arg_safe(old_mountpoint, strlen(old_mountpoint)))
	{
		fprintf(stderr, "invalid mountpoint [%s]\n", old_mountpoint);
		exit(1);
	}

	/*
	  GPHOME=os.environ.get('GPHOME')
	  SRC_GPPATH=". %s/greenplum_path.sh;" % GPHOME
	*/

	if (type == 'n')
	{
		/*
		 * We're now *really* taking ourselves into never-never land:
		 * go get a real root uid.
		 */
		setuid(0);

		if (!arg_safe(target_dev, strlen(target_dev)))
		{
			fprintf(stderr, "invalid dev string [%s]\n", target_dev);
			exit(1);
		}

		if (u_mount)
		{
			snprintf(cmd, sizeof(cmd), "ssh %s /bin/umount %s", old_host, old_mountpoint);

			fprintf(stdout, "unmount old host [%s]\n", old_host);
			fprintf(stdout, "unmount old mountpoint [%s]\n", old_mountpoint);

			fprintf(stdout, "unmount cmd [%s]\n", cmd);

			status = system(cmd);

			if (status != 0)
			{
				fprintf(stderr, "UMOUNT COMMAND FAILED %d (%s:%s)\n", status, old_host, old_mountpoint);
				exit(status);
			}

			/* 
			 * For our testing, simulated failures may have left the original mountpoint mounted!
			 * So we unmount it too. (this may return an error).
			 */
			snprintf(cmd, sizeof(cmd), "ssh %s /bin/umount %s", target_host, target_mountpoint);

			fprintf(stdout, "unmount new host [%s]\n", target_host);
			fprintf(stdout, "unmount new mountpoint [%s]\n", target_mountpoint);

			fprintf(stdout, "unmount cmd [%s]\n", cmd);

			status = system(cmd);

			/* We ignore the status here. The second un-mount may emit an error if had been unmounted already. */
		}

		snprintf(cmd, sizeof(cmd), "ssh %s /bin/mount %s %s", target_host, target_dev, target_mountpoint);

		fprintf(stdout, "cmd [%s]\n", cmd);

		status = system(cmd);

		if (status != 0)
		{
			exit(status);
		}
		/* done with NFS response */
	}
	else if (type == 'e')
	{
		int	fd;
		char	*WWN=NULL, *SP=NULL, *SG_NAME=NULL, *ARGS=NULL, *FSTYPE=NULL;
		char	tmpfilename[80];

		FILE	*tmpfile;
		char	dev_name[256];
		int	name_len;

		fprintf(stdout, "getuid %d geteuid %d\n", getuid(), geteuid());

		/* Parse out the names of the SP and WWN from our "device" */
		parse_target_dev(target_dev, &WWN, &SP, &SG_NAME, &ARGS, &FSTYPE);

		if (WWN == NULL)
		{
			fprintf(stderr, "No identifier for SAN-device.\n");
			exit(1);
		}

		if (u_mount)
		{
			/* We need to unfence before we can scan the target-device name! */
			if (SP != NULL && SG_NAME != NULL)
			{
				/* We issue out "unfence"/SG-reconnect operation here. */
				emc_fencer(SP, SG_NAME, target_host, 1);
			}
		}

		/* Get the name of the target device */

		/* 1. get a file */
		snprintf(tmpfilename, sizeof(tmpfilename), "/tmp/gpdb_inq_results.XXXXXX");

		fd = mkstemp(tmpfilename);
		if (fd < 0)
		{
			fprintf(stderr, "failed to create inq-results file: %d\n", errno);
			exit(1);
		}

		if (fchown(fd, getuid(), -1) < 0)
		{
			close(fd);
			unlink(tmpfilename);
			fprintf(stderr, "Device inquiry on host %s for WWN [%s]: inquiry command failed\n", target_host, WWN);
			exit(1);
		}

		/* 2. go get the inquiry results about the device -> "/dev/xxx" mapping */
		snprintf(cmd, sizeof(cmd), "ssh %s inq -no_dots -clar_wwn -f_powerpath | grep %s | awk '{print $1}' >> %s", target_host, WWN, tmpfilename);

		fprintf(stdout, "cmd [%s]\n", cmd);

		status = system(cmd);

		if (status != 0)
		{
			close(fd);
			unlink(tmpfilename);
			fprintf(stderr, "Device inquiry on host %s for WWN [%s]: inquiry command failed\n", target_host, WWN);
			exit(1);
		}

		tmpfile = fdopen(fd, "r");
		if (tmpfile == NULL)
		{
			close(fd);
			unlink(tmpfilename);
			fprintf(stderr, "Couldn't open %s for inquiry results\n", tmpfilename);
			exit(1);
		}

		memset(dev_name, 0, sizeof(dev_name));
		if (fgets(dev_name, sizeof(dev_name) - 1, tmpfile) == NULL)
		{
			int s_errno = errno;

			fclose(tmpfile);
			unlink(tmpfilename);
			fprintf(stderr, "Couldn't read dev_name from inquiry results: %d\n", s_errno);
			exit(1);
		}

		fclose(tmpfile);
		unlink(tmpfilename);

		name_len = strlen(dev_name);
		dev_name[name_len - 1] = 0; /* chop off \n left by fgets(). */

		if (dev_name[0] != '/')
		{
			fprintf(stderr, "device name [%s] does not appear to be valid\n", dev_name);
			exit(1);
		}

		/* 3) FENCE I/O ? */
		if (SP != NULL && SG_NAME != NULL)
		{
			if (u_mount)
			{
				/* NOTHING TO DO HERE, SEE ABOVE */
			}
			else
			{
				/* DO OUR I/O FENCING OPERATION HERE */
				emc_fencer(SP, SG_NAME, old_host, 0);
			}
		}

		/* 4) Do the mount: this is exactly like the NFS case now. */
		
		/*
		 * We're now *really* taking ourselves into never-never land:
		 * go get a real root uid.
		 */
		setuid(0);

		fprintf(stdout, "getuid %d geteuid %d\n", getuid(), geteuid());

		if (!arg_safe(dev_name, strlen(dev_name)))
		{
			fprintf(stderr, "invalid device-name [%s]\n", dev_name);
			exit(1);
		}

		if (u_mount)
		{
			snprintf(cmd, sizeof(cmd), "ssh %s /bin/umount %s", old_host, old_mountpoint);

			fprintf(stdout, "unmount old host [%s]\n", old_host);
			fprintf(stdout, "unmount old mountpoint [%s]\n", old_mountpoint);
			fprintf(stdout, "unmount cmd [%s]\n", cmd);

			status = system(cmd);

			if (status != 0)
			{
				fprintf(stderr, "Unmount command failed (%s:%s):%d '%s'\n", old_host, old_mountpoint, status, cmd);
				/* we try to proceed anyhow */
			}

			/* 
			 * For our testing, simulated failures may have left the original mountpoint mounted!
			 * So we unmount it too. (this may return an error).
			 */
			snprintf(cmd, sizeof(cmd), "ssh %s /bin/umount %s", target_host, target_mountpoint);

			fprintf(stdout, "unmount new host [%s]\n", target_host);
			fprintf(stdout, "unmount new mountpoint [%s]\n", target_mountpoint);
			fprintf(stdout, "unmount cmd [%s]\n", cmd);

			status = system(cmd);

			/* We ignore the status here. The second un-mount may emit an error if had been unmounted already. */
		}

		if (FSTYPE == NULL)
		{
			snprintf(cmd, sizeof(cmd), "ssh %s /bin/mount %s %s", target_host, dev_name, target_mountpoint);
		}
		else
		{
			if (!arg_safe(FSTYPE, strlen(FSTYPE)))
			{
				fprintf(stderr, "invalid filesystem type [%s]\n", FSTYPE);
				exit(1);
			}

			snprintf(cmd, sizeof(cmd), "ssh %s /bin/mount -t %s %s %s", target_host, FSTYPE, dev_name, target_mountpoint);
		}

		fprintf(stdout, "cmd [%s]\n", cmd);

		status = system(cmd);

		if (status != 0)
		{
			fprintf(stderr, "Mount command generated an error (%d) ('%s')\n", status, cmd);
			exit(0);
		}
	}

	exit(0);
}

static void
parse_target_dev(char *input, char **WWN, char **SP, char **SG_NAME, char **ARGS, char **FSTYPE)
{
	int len;
	int cur;
	char *pos, *sep, *val;

	*WWN = NULL;

	if (input == NULL)
	{
		return;
	}

	len = strlen(input);

	cur = 0;
	pos = input;
	while (cur < len)
	{
		sep = strchr(pos, ',');
		if (sep == NULL)
			cur = len; /* this is our last item */
		else
			*sep = 0;

		val = NULL;
		if (match(&pos, &val, "WWN"))
		{
			*WWN = val;
		}
		else if (match(&pos, &val, "SP"))
		{
			*SP = val;
		}
		else if (match(&pos, &val, "SG_NAME"))
		{
			*SG_NAME = val;
		}
		else if (match(&pos, &val, "ARGS"))
		{
			*ARGS = val;
		}
		else if (match(&pos, &val, "FSTYPE"))
		{
			*FSTYPE = val;
		}
		else
		{
			fprintf(stderr, "Failed parsing: %s\n", pos);
			exit(1);
		}
	}

	return;
}

static int
match(char **input, char **val, const char *token)
{
	char *p;
	int token_len;

	if (input == NULL)
	{
		return 0;
	}

	p = *input;

	if (p == NULL || val == NULL || token == NULL)
	{
		return 0;
	}

	*val = NULL;

	token_len = strlen(token);

	if (strlen(p) < token_len + 1)
	{
		return 0;
	}

	if (strncasecmp(p, token, token_len) == 0)
	{
		if (p[token_len] != '=')
		{
			return 0;
		}

		*val = p + token_len + 1; /* skip '=' */

		*input = *val + strlen(*val) + 1; /* skip NULL */

		return 1;
	}

	return 0;
}

static int
emc_check_fence(const char *sp_name, const char *sg_name, const char *host)
{
	char tmpfilename[80];
	char cmd[1000];
	char count_str[100];
	int count = 0;

	int	fd;
	int status;
	FILE *tmpfile=NULL;

	/* 1. get a file */
	snprintf(tmpfilename, sizeof(tmpfilename), "/tmp/gpdb_sg_lookup_results.XXXXXX");

	fd = mkstemp(tmpfilename);
	if (fd < 0)
	{
		fprintf(stderr, "failed to create sg_lookup-results file: %d\n", errno);

		return 0;
	}

	if (fchown(fd, getuid(), -1) < 0)
	{
		close(fd);
		unlink(tmpfilename);
		fprintf(stderr, "Storage-processor inquiry on sp %s storage-group %s host %s\n", sp_name, sg_name, host);

		return 0;
	}

	/* 2. go get the inquiry results about the device -> "/dev/xxx" mapping */
	snprintf(cmd, sizeof(cmd), "/opt/Navisphere/bin/naviseccli -h %s storagegroup -list -gname %s -host | /usr/bin/grep %s | /usr/bin/wc -l >> %s", sp_name, sg_name, host, tmpfilename);

	fprintf(stdout, "cmd [%s]\n", cmd);

	status = system(cmd);

	if (status != 0)
	{
		close(fd);
		unlink(tmpfilename);
		fprintf(stderr, "Storage-processor inquiry on sp %s storage-group %s host %s\n", sp_name, sg_name, host);

		return 0;
	}

	tmpfile = fdopen(fd, "r");
	if (tmpfile == NULL)
	{
		close(fd);
		unlink(tmpfilename);
		fprintf(stderr, "Couldn't open %s for storage-group lookup results\n", tmpfilename);

		return 0;
	}

	memset(count_str, 0, sizeof(count_str));
	if (fgets(count_str, sizeof(count_str) - 1, tmpfile) == NULL)
	{
		int s_errno = errno;

		fclose(tmpfile);
		unlink(tmpfilename);
		fprintf(stderr, "Couldn't read results from storage-group lookup: %d\n", s_errno);

		return 0;
	}

	fclose(tmpfile);
	unlink(tmpfilename);

	count = atoi(count_str);

	return (count > 0) ? count : 0;
}

static int
emc_fencer(const char *sp_name, const char *sg_name, const char *host, int unfence)
{
	char cmd[1000];
	int host_connections=0;
	int status;

	/* Fence off the host, we don't care what the current status on the storage-processor is. */
	if (unfence == 0)
	{
		snprintf(cmd, sizeof(cmd), "echo y | /opt/Navisphere/bin/naviseccli -h %s storagegroup -disconnecthost -host %s -gname %s", sp_name, host, sg_name);
		fprintf(stdout, "I/O Fencing command [%s]\n", cmd);

		status = system(cmd);

		if (status != 0)
		{
			fprintf(stderr, "Failed disconnect on storage processor, proceeding.\n");
		}

		host_connections = emc_check_fence(sp_name, sg_name, host);

		if (host_connections != 0)
		{
			fprintf(stderr, "I/O fenced, but %s is still connected to %s\n", host, sg_name);
		}

		return 0;
	}

	host_connections = emc_check_fence(sp_name, sg_name, host);

	if (host_connections > 0)
	{
		fprintf(stdout, "I/O Unfencing unnecessary, %s is already connected to storage-group %s\n", host, sg_name);

		return 0;
	}

	/* Unfence the host: if the host isn't fenced, we don't want to fence. */
	snprintf(cmd, sizeof(cmd), "echo y | /opt/Navisphere/bin/naviseccli -h %s storagegroup -connecthost -host %s -gname %s", sp_name, host, sg_name);
	fprintf(stdout, "I/O Unfencing command [%s]\n", cmd);

	status = system(cmd);

	if (status != 0)
	{
		fprintf(stderr, "Failed disconnect on storage processor, proceeding.\n");
	}

	host_connections = emc_check_fence(sp_name, sg_name, host);

	if (host_connections > 0)
	{
		fprintf(stdout, "I/O Unfencing complete, %s is connected to storage-group %s\n", host, sg_name);
	}

	return 0;
}
