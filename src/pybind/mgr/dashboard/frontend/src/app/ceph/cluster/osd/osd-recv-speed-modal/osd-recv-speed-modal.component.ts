import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';
import { forkJoin as observableForkJoin, of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { ConfigurationService } from '../../../../shared/api/configuration.service';
import { NotificationType } from '../../../../shared/enum/notification-type.enum';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { NotificationService } from '../../../../shared/services/notification.service';
import { OsdRecvSpeedModalPriorities } from './osd-recv-speed-modal.priorities';

@Component({
  selector: 'cd-osd-recv-speed-modal',
  templateUrl: './osd-recv-speed-modal.component.html',
  styleUrls: ['./osd-recv-speed-modal.component.scss']
})
export class OsdRecvSpeedModalComponent implements OnInit {
  osdRecvSpeedForm: CdFormGroup;
  priorities = OsdRecvSpeedModalPriorities.KNOWN_PRIORITIES;
  priorityAttrs = [];

  constructor(
    public bsModalRef: BsModalRef,
    private configService: ConfigurationService,
    private notificationService: NotificationService,
    private i18n: I18n
  ) {
    this.osdRecvSpeedForm = new CdFormGroup({
      priority: new FormControl(null, { validators: [Validators.required] }),
      customizePriority: new FormControl(false)
    });
    this.priorityAttrs = [
      {
        name: 'osd_max_backfills',
        text: this.i18n('Max Backfills')
      },
      {
        name: 'osd_recovery_max_active',
        text: this.i18n('Recovery Max Active')
      },
      {
        name: 'osd_recovery_max_single_start',
        text: this.i18n('Recovery Max Single Start')
      },
      {
        name: 'osd_recovery_sleep',
        text: this.i18n('Recovery Sleep')
      }
    ];

    this.priorityAttrs.forEach((attr) => {
      this.osdRecvSpeedForm.addControl(
        attr.name,
        new FormControl(null, { validators: [Validators.required] })
      );

      this.configService.get(attr.name).subscribe((data: any) => {
        if (data.desc !== '') {
          attr['desc'] = data.desc;
        }
      });
    });
  }

  ngOnInit() {
    this.getStoredPriority((priority) => {
      this.setPriority(priority);
    });
  }

  setPriority(priority: any) {
    const customPriority = _.find(this.priorities, (p) => {
      return p.name === 'custom';
    });

    if (priority.name === 'custom') {
      if (!customPriority) {
        this.priorities.push(priority);
      }
    } else {
      if (customPriority) {
        this.priorities.splice(this.priorities.indexOf(customPriority), 1);
      }
    }

    this.osdRecvSpeedForm.controls.priority.setValue(priority.name);
    Object.entries(priority.values).forEach(([name, value]) => {
      this.osdRecvSpeedForm.controls[name].setValue(value);
    });
  }

  onCustomizePriorityChange() {
    if (this.osdRecvSpeedForm.getValue('customizePriority')) {
      const values = {};
      this.priorityAttrs.forEach((attr) => {
        values[attr.name] = this.osdRecvSpeedForm.getValue(attr.name);
      });
      const customPriority = {
        name: 'custom',
        text: this.i18n('Custom'),
        values: values
      };
      this.setPriority(customPriority);
    } else {
      this.setPriority(this.priorities[0]);
    }
  }

  getStoredPriority(callbackFn: Function) {
    const observables = [];
    this.priorityAttrs.forEach((configName) => {
      observables.push(this.configService.get(configName.name));
    });

    observableForkJoin(observables)
      .pipe(
        mergeMap((configOptions) => {
          const result = {};
          configOptions.forEach((configOption) => {
            if (configOption && 'value' in configOption) {
              configOption.value.forEach((value) => {
                if (value['section'] === 'osd') {
                  result[configOption.name] = Number(value.value);
                }
              });
            }
          });
          return of(result);
        })
      )
      .subscribe((resp) => {
        const priority = _.find(this.priorities, (p) => {
          return _.isEqual(p.values, resp);
        });

        this.osdRecvSpeedForm.controls.customizePriority.setValue(false);

        if (priority) {
          return callbackFn(priority);
        }

        if (Object.entries(resp).length === 4) {
          this.osdRecvSpeedForm.controls.customizePriority.setValue(true);
          return callbackFn(Object({ name: 'custom', text: this.i18n('Custom'), values: resp }));
        }

        return callbackFn(this.priorities[0]);
      });
  }

  onPriorityChange(selectedPriorityName) {
    const selectedPriority =
      _.find(this.priorities, (p) => {
        return p.name === selectedPriorityName;
      }) || this.priorities[0];

    this.setPriority(selectedPriority);
  }

  submitAction() {
    const options = {};
    this.priorityAttrs.forEach((attr) => {
      options[attr.name] = { section: 'osd', value: this.osdRecvSpeedForm.getValue(attr.name) };
    });

    this.configService.bulkCreate({ options: options }).subscribe(
      () => {
        this.notificationService.show(
          NotificationType.success,
          this.i18n('OSD recovery speed priority "{{value}}" was set successfully.', {
            value: this.osdRecvSpeedForm.getValue('priority')
          }),
          this.i18n('OSD recovery speed priority')
        );
        this.bsModalRef.hide();
      },
      () => {
        this.bsModalRef.hide();
      }
    );
  }
}
